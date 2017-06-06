/*
 * Adobe Media Manifest (F4M) File Parser
 * Copyright (c) 2013 Cory McCarthy
 *
 * This file is part of FFmpeg.
 *
 * FFmpeg is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * FFmpeg is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with FFmpeg; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA
 */

/**
 * @file
 * @brief Adobe Media Manifest (F4M) File Parser
 * @author Cory McCarthy
 * @see http://wwwimages.adobe.com/www.adobe.com/content/dam/Adobe/en/devnet/hds/pdfs/adobe-media-manifest-specification.pdf
 */

#include "f4mmanifest.h"
#include "libavutil/avstring.h"
#include "libavutil/base64.h"
#include <roxml.h>

static int f4m_parse_bootstrap_info_node(node_t * node, F4MBootstrapInfo *bootstrap_info)
{
    const char  *p;
    uint8_t *dst;
    int ret;
    node_t *attr;

    attr = roxml_get_attr(node, "id", 0);
    p =roxml_get_content(attr,NULL,0,NULL);
    if(p) {
        av_strlcpy(bootstrap_info->id, p, sizeof(bootstrap_info->id));
    }

    attr = roxml_get_attr(node, "url", 0);
    p =roxml_get_content(attr,NULL,0,NULL);
    if(p) {
        av_strlcpy(bootstrap_info->url, p, sizeof(bootstrap_info->url));
    }

    attr = roxml_get_attr(node, "profile", 0);
    p =roxml_get_content(attr,NULL,0,NULL);
    if(p) {
        av_strlcpy(bootstrap_info->profile, p, sizeof(bootstrap_info->profile));
    }

    p = roxml_get_content(node, NULL, 0, NULL);
    if(p) {
        dst = av_mallocz(sizeof(uint8_t)*strlen(p));
        if(!dst)
            return AVERROR(ENOMEM);

        if((ret = av_base64_decode(dst, p, strlen(p))) < 0) {
            av_log(NULL, AV_LOG_ERROR, "f4mmanifest Failed to decode bootstrap node base64 metadata, ret: %d \n", ret);
            av_free(dst);
            return ret;
        }

        bootstrap_info->metadata = av_mallocz(sizeof(uint8_t)*ret);
        if(!bootstrap_info->metadata)
            return AVERROR(ENOMEM);

        bootstrap_info->metadata_size = ret;
        memcpy(bootstrap_info->metadata, dst, ret);

        av_free(dst);
    }

    return 0;
}

static int f4m_parse_metadata_node(node_t * node, F4MMedia *media)
{
    const char  *p = NULL;
    uint8_t *dst;
    int ret;

    node_t * metadata_node = roxml_get_chld(node, NULL, 0);
    while(metadata_node) {
        if(!strcmp(roxml_get_name(metadata_node, NULL, 0), "metadata")) {
            p = roxml_get_content(metadata_node, NULL, 0, NULL);
            break;
        }
        metadata_node = roxml_get_next_sibling(metadata_node);
    }

    if(!p)
        return 0;

    dst = av_mallocz(sizeof(uint8_t)*strlen(p));
    if(!dst)
        return AVERROR(ENOMEM);

    if((ret = av_base64_decode(dst, p, strlen(p))) < 0) {
        av_log(NULL, AV_LOG_ERROR, "f4mmanifest Failed to decode base64 metadata, ret: %d \n", ret);
        av_free(dst);
        return ret;
    }

    media->metadata = av_mallocz(sizeof(uint8_t)*ret);
    if(!media->metadata)
        return AVERROR(ENOMEM);

    media->metadata_size = ret;
    memcpy(media->metadata, dst, ret);

    av_free(dst);

    return 0;
}

static int f4m_parse_media_node(node_t * node, F4MMedia *media)
{
    const char  *p;
    int ret;
    node_t * attr;

    attr = roxml_get_attr(node, "bitrate", 0);
    p =roxml_get_content(attr,NULL,0,NULL);
    if(p) {
        media->bitrate = strtoul(p, NULL, 10);
    }

    attr = roxml_get_attr(node, "url", 0);
    p =roxml_get_content(attr,NULL,0,NULL);
    if(p) {
        av_strlcpy(media->url, p, sizeof(media->url));
    }

    attr = roxml_get_attr(node, "bootstrapInfoId", 0);
    p =roxml_get_content(attr,NULL,0,NULL);
    if(p) {
        av_strlcpy(media->bootstrap_info_id, p, sizeof(media->bootstrap_info_id));
    }

    if((ret = f4m_parse_metadata_node(node, media)) < 0) {
        return ret;
    }

    return 0;
}

static int f4m_parse_manifest_node(node_t * root_node, F4MManifest *manifest)
{
    F4MBootstrapInfo *bootstrap_info;
    F4MMedia *media;
    node_t * node;
    const char  *node_content;
    int ret = 0,chld_idx=0;

    for (chld_idx=0; chld_idx<roxml_get_chld_nb(root_node); chld_idx++){
	node = roxml_get_chld(root_node, NULL, chld_idx);
	const char * node_name = roxml_get_name(node, NULL, 0);
        if(!strcmp(node_name, "text"))
            continue;

	node_content = roxml_get_content(node, NULL, 0, NULL);

        if(!strcmp(node_name, "id") && node_content) {
            av_strlcpy(manifest->id, node_content, sizeof(manifest->id));
        } else if(!strcmp(node_name, "streamType") && node_content) {
            av_strlcpy(manifest->stream_type, node_content, sizeof(manifest->stream_type));
        } else if(!strcmp(node_name, "bootstrapInfo")) {
            bootstrap_info = av_mallocz(sizeof(F4MBootstrapInfo));
            if(!bootstrap_info)
                return AVERROR(ENOMEM);
            manifest->bootstraps[manifest->nb_bootstraps++] = bootstrap_info;
            ret = f4m_parse_bootstrap_info_node(node, bootstrap_info);
        } else if(!strcmp(node_name, "media")) {
            media = av_mallocz(sizeof(F4MMedia));
            if(!media)
                return AVERROR(ENOMEM);
            manifest->media[manifest->nb_media++] = media;
            ret = f4m_parse_media_node(node, media);
        } else if (!strcmp(node_name, "duration")) {
	    double duration = strtod(node_content, NULL);
	    manifest->duration = duration * AV_TIME_BASE;
	}

        if(ret < 0)
            return ret;
    }

    return 0;
}

static int f4m_parse_xml_file(uint8_t *buffer, int size, F4MManifest *manifest)
{
    node_t * doc;
    node_t * root_node;
    int ret;

    doc = roxml_load_buf(buffer);
    if(!doc) {
        return -1;
    }

    doc = roxml_get_root(doc);
    root_node = roxml_get_chld(doc, NULL, 0);
    if(!root_node) {
        av_log(NULL, AV_LOG_ERROR, "f4mmanifest Root element not found \n");
        roxml_close(doc);
        return -1;
    }
    const char * root_node_name = roxml_get_name(root_node, NULL, 0);
    if(strcmp(root_node_name, "manifest")) {
        av_log(NULL, AV_LOG_ERROR, "f4mmanifest Root element is not named manifest, name = %s \n", root_node_name);
        roxml_close(doc);
        return -1;
    }

    ret = f4m_parse_manifest_node(root_node, manifest);
    roxml_close(doc);

    return ret;
}

int ff_parse_f4m_manifest(uint8_t *buffer, int size, F4MManifest *manifest)
{
    return f4m_parse_xml_file(buffer, size, manifest);
}

int ff_free_manifest(F4MManifest *manifest)
{
    F4MBootstrapInfo *bootstrap_info;
    F4MMedia *media;
    int i;

    for(i = 0; i < manifest->nb_bootstraps; i++) {
        bootstrap_info = manifest->bootstraps[i];
        av_freep(&bootstrap_info->metadata);
        av_freep(&bootstrap_info);
    }

    for(i = 0; i < manifest->nb_media; i++) {
        media = manifest->media[i];
        av_freep(&media->metadata);
        av_freep(&media);
    }

    memset(manifest, 0x00, sizeof(F4MManifest));

    return 0;
}
