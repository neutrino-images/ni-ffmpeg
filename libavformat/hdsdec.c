/*
 * Adobe HTTP Dynamic Streaming (HDS) demuxer
 * Copyright (c) 2013 Cory McCarthy
 * Copyright (c) 2014 martii
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
 * @brief Adobe HTTP Dynamic Streaming (HDS) demuxer
 * @author Cory McCarthy
 * @see http://www.adobe.com/devnet/hds.html
 * @see http://wwwimages.adobe.com/www.adobe.com/content/dam/Adobe/en/devnet/hds/pdfs/adobe-hds-specification.pdf
 * @see http://wwwimages.adobe.com/www.adobe.com/content/dam/Adobe/en/devnet/hds/pdfs/adobe-media-manifest-specification.pdf
 * @see http://download.macromedia.com/f4v/video_file_format_spec_v10_1.pdf
 *
 * @note Link for a HDS test player below:
 * @see http://mediapm.edgesuite.net/edgeflash/public/zeri/debug/Main.html
 *
 * @note Test streams are below:
 * @test http://multiplatform-f.akamaihd.net/z/multi/april11/hdworld/hdworld_,512x288_450_b,640x360_700_b,768x432_1000_b,1024x576_1400_m,1280x720_1900_m,1280x720_2500_m,1280x720_3500_m,.mp4.csmil/manifest.f4m?hdcore
 * @test http://multiplatform-f.akamaihd.net/z/multi/april11/cctv/cctv_,512x288_450_b,640x360_700_b,768x432_1000_b,1024x576_1400_m,1280x720_1900_m,1280x720_2500_m,1280x720_3500_m,.mp4.csmil/manifest.f4m?hdcore
 * @test http://multiplatform-f.akamaihd.net/z/multi/april11/sintel/sintel-hd_,512x288_450_b,640x360_700_b,768x432_1000_b,1024x576_1400_m,1280x720_1900_m,1280x720_2500_m,1280x720_3500_m,.mp4.csmil/manifest.f4m?hdcore
 * @test http://multiplatform-f.akamaihd.net/z/multi/akamai10year/Akamai_10_Year_,200,300,600,800,1000,1500,2500,4000,k.mp4.csmil/manifest.f4m?hdcore
 * @test http://zerihdndemo-f.akamaihd.net/z/h264/seeker/LegendofSeeker_16x9_24fps_H264_,400K,650K,1Mbps,1.4Mbps,1.8Mbps,2.5Mbps,.mp4.csmil/manifest.f4m?hdcore
 * @test http://multiplatform-f.akamaihd.net/z/multi/will/bunny/big_buck_bunny_,640x360_400,640x360_700,640x360_1000,950x540_1500,1280x720_2000,1280x720_3000,.f4v.csmil/manifest.f4m?hdcore
 * @test http://multiplatform-f.akamaihd.net/z/multi/companion/nba_game/nba_game.mov_,300,600,800,1000,2500,4000,9000,k.mp4.csmil/manifest.f4m?hdcore
 * @test http://multiplatform-f.akamaihd.net/z/multi/companion/big_bang_theory/big_bang_theory.mov_,300,600,800,1000,2500,4000,9000,k.mp4.csmil/manifest.f4m?hdcore
 * @test http://multiplatform-f.akamaihd.net/z/multi/shuttle/shuttle_,300,600,800,1000,k.mp4.csmil/manifest.f4m?hdcore
 * @test http://multiplatform-f.akamaihd.net/z/multi/up_trailer/up_trailer_720p_,300,600,800,1000,k.mp4.csmil/manifest.f4m?hdcore
 * @test http://multiformatlive-f.akamaihd.net/z/demostream_1@2131/manifest.f4m?hdcore
 * @test http://zerihdndemo-f.akamaihd.net/z/h264/darkknight/darkknight.smil/manifest.f4m?hdcore
 * @test http://zerihdndemo-f.akamaihd.net/z/h264/amours/amours.smil/manifest.f4m?hdcore
 * @test http://zerihdndemo-f.akamaihd.net/z/h264/robinhood/robinhood.smil/manifest.f4m?hdcore
 * @test http://zerihdndemo-f.akamaihd.net/z/h264/wallstreet/wallstreet.smil/manifest.f4m?hdcore
 * @test http://zerihdndemo-f.akamaihd.net/z/h264/rockandroll/rockandroll.smil/manifest.f4m?hdcore
 * @test http://184.72.239.149/vod/smil:bigbuckbunny.smil/manifest.f4m
 */

#include "avformat.h"
#include "internal.h"
#include "url.h"
#include "avio_internal.h"
#include "libavutil/avstring.h"
#include "libavutil/parseutils.h"
#include "libavutil/opt.h"
#include "libavutil/dict.h"
#include "libavutil/time.h"

#include "amfmetadata.h"
#include "f4mmanifest.h"
#include "f4fbox.h"
#include "flvtag.h"

#include <unistd.h>
#include <pthread.h>
#include <semaphore.h>
#include <alloca.h>

#define MAX_NB_SAMPLES 1024

struct HDSDownloadThreadData
{
    pthread_mutex_t mutex;			// protects access to this struct
    pthread_t thread;				// download thread id
    sem_t *to_thread;				// send signal to thread
    sem_t *to_caller;				// send signal to caller
    sem_t _to_thread;				// send signal to thread
    sem_t _to_caller;				// send signal to caller
    char *cookies;				// cookies, if any
    char *url;					// current url
    int run;					// thread will run until == 0
    int abort;					// setting this to !=0 aborts download
    uint8_t *buffer;				// set by thread, unset by thread or caller
    uint32_t buflen;				// set by thread, unset by thread or caller
    AVIOInterruptCB *interrupt_callback;
    AVIOInterruptCB *abort_callback;
    AVIOInterruptCB download_abort_callback;
};

typedef struct HDSBootstrapInfo {
    char id[MAX_URL_SIZE];
    char url[MAX_URL_SIZE];
    char profile[MAX_URL_SIZE];
    char *quality;

    F4FBox box;
} HDSBootstrapInfo;

typedef struct HDSMedia {
    int media_index;
    int bitrate;
    char url[MAX_URL_SIZE];
    HDSBootstrapInfo *bootstrap_info;

    AVStream *audio_stream;
    AVStream *video_stream;

    int nb_samples;
    FLVMediaSample *samples[MAX_NB_SAMPLES];
    int sample_index;

    unsigned int nb_fragments_read;

    struct HDSDownloadThreadData download_data;
} HDSMedia;

typedef struct HDSContext {
    char id[MAX_URL_SIZE];
    char base_url[MAX_URL_SIZE];
    int is_live;
    int last_media_index;

    int nb_bootstraps;
    HDSBootstrapInfo *bootstrap_info[MAX_NB_BOOTSTRAPS];

    int nb_media;
    HDSMedia *media[MAX_NB_MEDIA];

    int64_t seek_timestamp;
    char *cookies;
} HDSContext;

static void construct_bootstrap_url(const char *base_url, const char *bootstrap_url,
    const char *suffix, char *url_out, size_t url_size)
{
    char *p;

    p = url_out;
    p += av_strlcat(p, base_url, url_size);
    p += av_strlcat(p, bootstrap_url, url_size);
    p += av_strlcat(p, suffix, url_size);
}

static int download_bootstrap(AVFormatContext *s, HDSBootstrapInfo *bootstrap,
    uint8_t **buffer_out, int *buffer_size_out)
{
    HDSContext *c = s->priv_data;
    URLContext *puc = NULL;
    AVDictionary *opts = NULL;
    char url[MAX_URL_SIZE];
    uint8_t *buffer;
    int buffer_size;
    int ret;

    memset(url, 0x00, sizeof(url));

    if(!av_stristr(bootstrap->url, "?") && av_stristr(s->filename, "?")) {
	construct_bootstrap_url(c->base_url, bootstrap->url, av_stristr(s->filename, "?"), url, MAX_URL_SIZE);
    } else {
	construct_bootstrap_url(c->base_url, bootstrap->url, "", url, MAX_URL_SIZE);
    }

    av_dict_set(&opts, "cookies", c->cookies, 0);
    ret = ffurl_open(&puc, url, AVIO_FLAG_READ, &s->interrupt_callback, &opts);
    av_dict_free(&opts);

    if(ret < 0) {
	av_log(NULL, AV_LOG_ERROR, "hds Failed to start downloading bootstrap, ret: %d\n", ret);
	return ret;
    }

    buffer_size = ffurl_size(puc);
    buffer = av_mallocz(buffer_size+FF_INPUT_BUFFER_PADDING_SIZE);
    if(!buffer)
	return AVERROR(ENOMEM);

    if((ret = ffurl_read_complete(puc, buffer, buffer_size)) < 0) {
	av_log(NULL, AV_LOG_ERROR, "hds Failed to download bootstrap, ret: %d\n", ret);
	av_free(buffer);
	return ret;
    }

    if (c->cookies)
	av_freep(&c->cookies);
    av_opt_get(puc->priv_data, "cookies", 0, (uint8_t **) &c->cookies);
    if (c->cookies && !strlen(c->cookies))
	av_freep(&c->cookies);

    if((ret = ffurl_close(puc)) < 0) {
	av_log(NULL, AV_LOG_ERROR, "hds Failed to finish downloading bootstrap, ret: %d\n", ret);
	av_free(buffer);
	return ret;
    }

    if(buffer_out)
	*buffer_out = buffer;
    else
	av_free(buffer);

    if(buffer_size_out)
	*buffer_size_out = buffer_size;

    return 0;
}

static int create_bootstrap_info(AVFormatContext *s, F4MBootstrapInfo *f4m_bootstrap_info)
{
    HDSContext *c = s->priv_data;
    HDSBootstrapInfo *bootstrap_info;
    uint8_t *buffer;
    int buffer_size, ret;

    bootstrap_info = av_mallocz(sizeof(HDSBootstrapInfo));
    if(!bootstrap_info)
	return AVERROR(ENOMEM);

    c->bootstrap_info[c->nb_bootstraps++] = bootstrap_info;

    memcpy(bootstrap_info->id, f4m_bootstrap_info->id, sizeof(bootstrap_info->id));
    memcpy(bootstrap_info->url, f4m_bootstrap_info->url, sizeof(bootstrap_info->url));
    memcpy(bootstrap_info->profile, f4m_bootstrap_info->profile, sizeof(bootstrap_info->profile));

    buffer = f4m_bootstrap_info->metadata;
    buffer_size = f4m_bootstrap_info->metadata_size;

    if(buffer_size > 0) {
	if((ret = ff_parse_f4f_box(buffer, buffer_size, &(bootstrap_info->box))) < 0) {
	    av_log(NULL, AV_LOG_ERROR, "hds Failed to parse metadata bootstrap box, ret: %d\n", ret);
	    return ret;
	}
    } else {
	if((ret = download_bootstrap(s, bootstrap_info, &buffer, &buffer_size)) < 0) {
	    av_log(NULL, AV_LOG_ERROR, "hds Failed to download bootstrap, ret: %d\n", ret);
	    return ret;
	}

	if((ret = ff_parse_f4f_box(buffer, buffer_size, &(bootstrap_info->box))) < 0) {
	    av_log(NULL, AV_LOG_ERROR, "hds Failed to parse downloaded bootstrap box, ret: %d\n", ret);
	    return ret;
	}
    }

    return 0;
}

static int create_streams(AVFormatContext *s, HDSMedia *media, AMFMetadata *metadata, int i)
{
    if (metadata->video_codec_id) {
	AVStream *st = avformat_new_stream(s, NULL);
	if(!st)
	    return AVERROR(ENOMEM);

	media->video_stream = st;
	avpriv_set_pts_info(st, 32, 1, 1000);
	st->discard = AVDISCARD_ALL;
	st->id = 0 + 2 * i;
	st->codec->codec_type = AVMEDIA_TYPE_VIDEO;
	st->codec->codec_id = metadata->video_codec_id;
	st->codec->width = metadata->width;
	st->codec->height = metadata->height;
	st->codec->bit_rate = metadata->video_data_rate * 1000;
    }

    if (metadata->audio_codec_id) {
	AVStream *st = avformat_new_stream(s, NULL);
	if(!st)
	    return AVERROR(ENOMEM);

	media->audio_stream = st;
	avpriv_set_pts_info(st, 32, 1, 1000);
	st->discard = AVDISCARD_ALL;
	st->id = 1 + 2 * i;
	st->codec->codec_type = AVMEDIA_TYPE_AUDIO;
	st->codec->codec_id = metadata->audio_codec_id;
	st->codec->channels = metadata->nb_audio_channels;
	st->codec->channel_layout = (st->codec->channels == 2) ?  AV_CH_LAYOUT_STEREO : AV_CH_LAYOUT_MONO;
	st->codec->sample_rate = metadata->audio_sample_rate;
	st->codec->sample_fmt = AV_SAMPLE_FMT_S16;
	st->codec->bit_rate = metadata->audio_data_rate * 1000;
	st->need_parsing = metadata->audio_stream_need_parsing;
    }

    return 0;
}

static int create_media(AVFormatContext *s, F4MMedia *f4m_media, int i)
{
    HDSContext *c = s->priv_data;
    HDSMedia *media;
    AMFMetadata metadata;
    int ret, j;

    media = av_mallocz(sizeof(HDSMedia));
    if(!media)
	return AVERROR(ENOMEM);

    c->media[c->nb_media++] = media;

    media->media_index = i;
    media->bitrate = f4m_media->bitrate;
    memcpy(media->url, f4m_media->url, sizeof(media->url));

    for(j = 0; j < c->nb_bootstraps; j++) {
	if(!av_strcasecmp(f4m_media->bootstrap_info_id, c->bootstrap_info[j]->id))
	    continue;
	media->bootstrap_info = c->bootstrap_info[j];
	break;
    }

    memset(&metadata, 0x00, sizeof(AMFMetadata));
    metadata.nb_audio_channels = 1;
    if((ret = ff_parse_amf_metadata(f4m_media->metadata, f4m_media->metadata_size, &metadata)) < 0) {
	av_log(NULL, AV_LOG_ERROR, "hds Failed to parse metadata, ret: %d\n", ret);
	return ret;
    }

    return create_streams(s, media, &metadata, i);
}

static int create_pmt(AVFormatContext *s)
{
    HDSContext *c = s->priv_data;
    HDSMedia *media;
    AVProgram *p;
    int i, j;

    j = 0;
    for(i = 0; i < c->nb_media; i++) {
	media = c->media[i];

	p = av_new_program(s, j++);
	if(!p)
	    return AVERROR(ENOMEM);

	av_dict_set(&p->metadata,"name",
	    av_asprintf("%d kbit/s", media->bitrate), 0);

	if (media->video_stream)
	    ff_program_add_stream_index(s, p->id, media->video_stream->index);
	if (media->audio_stream)
	    ff_program_add_stream_index(s, p->id, media->audio_stream->index);
    }

    return 0;
}

static void download_thread_start(AVFormatContext *s, HDSMedia *media);

static int initialize_context(AVFormatContext *s, F4MManifest *manifest)
{
    HDSContext *c = s->priv_data;
    F4MBootstrapInfo *f4m_bootstrap_info;
    F4MMedia *f4m_media;
    int i, ret;

    for(i = 0; i < manifest->nb_bootstraps; i++) {
	f4m_bootstrap_info = manifest->bootstraps[i];
	if((ret = create_bootstrap_info(s, f4m_bootstrap_info)) < 0) {
	    av_log(NULL, AV_LOG_ERROR, "hds Failed to create bootstrap_info, ret: %d\n", ret);
	    return ret;
	}
    }

    for(i = 0; i < manifest->nb_media; i++) {
	f4m_media = manifest->media[i];
	if((ret = create_media(s, f4m_media, i)) < 0) {
	    av_log(NULL, AV_LOG_ERROR, "hds Failed to create media, ret: %d\n", ret);
	    return ret;
	}
    }

    if((ret = create_pmt(s)) < 0) {
	av_log(NULL, AV_LOG_ERROR, "hds Failed to create PMT, ret: %d\n", ret);
	return ret;
    }

    if(!av_strcasecmp(manifest->stream_type, "live"))
	c->is_live = 1;

    s->duration = manifest->duration;
    c->seek_timestamp = AV_NOPTS_VALUE;

    for(i = 0; i < c->nb_media; i++)
	download_thread_start(s, c->media[i]);

    return 0;
}

//#define HDS_ENABLE_LOG_CALLBACK
#ifdef HDS_ENABLE_LOG_CALLBACK
static void log_callback(void *ptr __attribute__ ((unused)), int lvl __attribute__ ((unused)), const char *format, va_list ap)
{
    vfprintf(stderr, format, ap);
}
#endif

static int hds_read_header(AVFormatContext *s)
{
    HDSContext *c = s->priv_data;
    AVIOContext *in = s->pb;
    F4MManifest manifest;
    int64_t filesize;
    uint8_t *buf;
    char *p;
    int ret;
#ifdef HDS_ENABLE_LOG_CALLBACK
    av_log_set_callback(log_callback);
#endif

    p = av_stristr(s->filename, "manifest.f4m");
    if(!p) {
	av_log(NULL, AV_LOG_ERROR, "hds \"manifest.f4m\" is not a substring of \"%s\"\n", s->filename);
	return -1;
    }
    av_strlcpy(c->base_url, s->filename, p - s->filename + 1);

    filesize = avio_size(in);
    if(filesize <= 0)
	return -1;

    buf = av_mallocz(filesize*sizeof(uint8_t));
    if(!buf)
	return AVERROR(ENOMEM);

    avio_read(in, buf, filesize);

    memset(&manifest, 0x00, sizeof(F4MManifest));
    ret = ff_parse_f4m_manifest(buf, filesize, &manifest);

    av_free(buf);

    if (ret > -1)
	ret = initialize_context(s, &manifest);

    ff_free_manifest(&manifest);

    return ret;
}

static void construct_fragment_url(const char *base_url, const char *media_url,
    unsigned int segment, unsigned int fragment, const char *suffix, char *url_out, size_t url_size)
{
    char *p;
    char *fragment_str;

    p = url_out;
    p += av_strlcat(p, base_url, url_size);
    p += av_strlcat(p, media_url, url_size);

    fragment_str = av_asprintf("Seg%u-Frag%u", segment, fragment);
    p += av_strlcat(p, fragment_str, url_size);
    av_free(fragment_str);

    p += av_strlcat(p, suffix, url_size);
}

static int get_fragment_offset(HDSMedia *media, int64_t timestamp)
{
    F4FBootstrapInfoBox *abst = &(media->bootstrap_info->box.abst);
    int fragments_max = 0;
    int i, j;

    for(i = 0; i < abst->nb_segment_run_table_boxes; i++) {
	F4FSegmentRunTableBox *asrt = abst->segment_run_table_boxes[i];
	int found = 0;
	if (asrt->nb_quality_entries && media->bootstrap_info->quality)
	    for(j = 0; !found && j < asrt->nb_quality_entries; j++)
		found = !strcmp(asrt->quality_entries[j], media->bootstrap_info->quality);
	else
	    found = 1;

	if (found) {
	    F4FSegmentRunEntry *segment_entry = asrt->segment_run_entries[asrt->nb_segment_run_entries - 1];
	    fragments_max = segment_entry->fragments_per_segment;
	    break;
	}
    }

    for(i = 0; i < abst->nb_fragment_run_table_boxes; i++) {
	int found = 0;
	F4FFragmentRunTableBox *afrt = abst->fragment_run_table_boxes[i];
	if (afrt->nb_quality_entries && media->bootstrap_info->quality)
	    for(j = 0; !found && j < afrt->nb_quality_entries; j++)
		found = !strcmp(afrt->quality_entries[j], media->bootstrap_info->quality);
	else
	    found = 1;

	if (found) {
	    for(j = 0; j < afrt->nb_fragment_run_entries; j++) {
		F4FFragmentRunEntry *fre = afrt->fragment_run_entries[j];
		int fragcount;
		if (j + 1 < afrt->nb_fragment_run_entries) {
		    fragcount = afrt->fragment_run_entries[j + 1]->first_fragment - fre->first_fragment;
		    fragments_max -= fragcount;
		} else
		    fragcount = fragments_max;

		if (timestamp >= fre->first_fragment_time_stamp && timestamp <= fre->first_fragment_time_stamp + fragcount * fre->fragment_duration)
		    return fre->first_fragment + (timestamp - fre->first_fragment_time_stamp)/fre->fragment_duration;
	    }
	    break;
	}
    }
    return 0;
}

static int get_segment_fragment(int is_live, HDSMedia *media, unsigned int *segment_out, unsigned int *fragment_out)
{
    F4FBootstrapInfoBox *abst = &(media->bootstrap_info->box.abst);
    unsigned int segment = ~0, fragment = ~0, fragments_max = 0;
    int i, j, skip;

    if (is_live) {
	// FIXME. This is a crude hack.
	*segment_out = 1;
	*fragment_out = media->nb_fragments_read;
	return 0;
    }

    skip = media->nb_fragments_read;
    for(i = 0; segment == ~0 && i < abst->nb_segment_run_table_boxes; i++) {
	F4FSegmentRunTableBox *asrt = abst->segment_run_table_boxes[i];
	int found = 0;
	if (asrt->nb_quality_entries && media->bootstrap_info->quality)
	    for(j = 0; !found && j < asrt->nb_quality_entries; j++)
		found = !strcmp(asrt->quality_entries[j], media->bootstrap_info->quality);
	else
	    found = 1;

	if (found) {
	    for (j = 0; segment == ~0 && j < asrt->nb_segment_run_entries; j++) {
		F4FSegmentRunEntry *segment_entry = asrt->segment_run_entries[j];

		if (segment_entry->fragments_per_segment < skip) {
		    skip -= segment_entry->fragments_per_segment;
		    fragments_max = segment_entry->fragments_per_segment;
		} else
		    segment = segment_entry->first_segment;
	    }
	    break;
	}
    }

    if(segment == ~0) {
	av_log(NULL, AV_LOG_ERROR, "hds segment entry for next fragment (%u) not found, skip: %d\n", media->nb_fragments_read, skip);
	return 0;
    }

    skip = media->nb_fragments_read;
    for(i = 0; fragment == ~0 && i < abst->nb_fragment_run_table_boxes; i++) {
	int found = 0;
	F4FFragmentRunTableBox *afrt = abst->fragment_run_table_boxes[i];
	if (afrt->nb_quality_entries && media->bootstrap_info->quality)
	    for(j = 0; !found && j < afrt->nb_quality_entries; j++)
		found = !strcmp(afrt->quality_entries[j], media->bootstrap_info->quality);
	else
	    found = 1;

	if (found) {
	    for(j = 0; fragment == ~0 && j < afrt->nb_fragment_run_entries; j++) {
		int fragcount;
		F4FFragmentRunEntry *fre = afrt->fragment_run_entries[j];
		if (j + 1 < afrt->nb_fragment_run_entries) {
		    fragcount = afrt->fragment_run_entries[j + 1]->first_fragment - fre->first_fragment;
		    fragments_max -= fragcount;
		} else
		    fragcount = fragments_max;

		if (fragcount < skip)
		    fragcount -= skip;
		else {
		    fragment = fre->first_fragment + skip;
		    skip = 0;
		}
	    }
	    break;
	}
    }

    if (!is_live && skip > 0) {
	av_log(NULL, AV_LOG_ERROR, "hds fragment %d fragments beyond EOF\n", skip);
	return AVERROR_EOF;
    }

    if(fragment == ~0) {
	av_log(NULL, AV_LOG_ERROR, "hds fragment entry not found\n");
	return -1;
    }

    if(segment_out)
	*segment_out = segment;
    if(fragment_out)
	*fragment_out = fragment;

    return 0;
}

static int download_abort_callback_function(void *opaque)
{
    HDSMedia *media = (HDSMedia *) opaque;
    return media->download_data.abort || ff_check_interrupt(media->download_data.interrupt_callback);
}

static void *download_thread(void *opaque)
{
    HDSMedia *media = (HDSMedia *) opaque;

    while (media->download_data.run && !download_abort_callback_function(media)) {
	AVDictionary *opts = NULL;
	URLContext *puc = NULL;
	uint8_t *buffer = NULL;
	int buffer_size = 0, ret;
	char *url = NULL;
	int url_len;
	int tries_left = 15;

	if (sem_wait(media->download_data.to_thread))
	    break;
	if (!media->download_data.run)
	    continue;

	pthread_mutex_lock(&media->download_data.mutex);
	media->download_data.abort = 0;
	if (media->download_data.buffer)
	    av_freep(&media->download_data.buffer);
	media->download_data.buflen = 0;
	// generate local copies from HDSDownloadThreadData
	if (media->download_data.cookies)
	    av_dict_set(&opts, "cookies", media->download_data.cookies, 0);
	url_len = strlen(media->download_data.url) + 1;
	url = alloca(url_len);
	strncpy(url, media->download_data.url, url_len);
	pthread_mutex_unlock(&media->download_data.mutex);
	//av_log(NULL, AV_LOG_DEBUG, "%s %d: downloading %s\n", __FILE__,__LINE__, url);

	// initiate download
	do {
	    ret = ffurl_open(&puc, url, AVIO_FLAG_READ, &media->download_data.download_abort_callback, &opts);
	    if (ret < 0) {
		sleep(1);
		if (download_abort_callback_function(media))
		    break;
		sleep(1);
		tries_left--;
	    }
	} while (ret < 0 && media->download_data.run && !download_abort_callback_function(media) && tries_left > 0);

	if (opts)
	    av_dict_free(&opts);

	if(ret < 0) {
	    av_log(NULL, AV_LOG_ERROR, "hds Failed to start downloading url:%s, ret:%d\n", url, ret);
	} else {
	    buffer_size = ffurl_size(puc);
	    if (buffer_size > -1)
		buffer = av_mallocz(buffer_size+FF_INPUT_BUFFER_PADDING_SIZE);
	    if(!buffer)
		av_log(NULL, AV_LOG_DEBUG, "hds Failed to allocate %d bytes buffer\n", buffer_size);
	}

	if(buffer && (ret = ffurl_read_complete(puc, buffer, buffer_size)) < 0) {
	    av_log(NULL, AV_LOG_ERROR, "hds Failed to download fragment, ret: %d\n", ret);
	    av_freep(&buffer);
	}

	pthread_mutex_lock(&media->download_data.mutex);
	if (media->download_data.abort || !buffer) {
	    media->download_data.abort = 0;
	} else {
	    //av_log(NULL, AV_LOG_DEBUG, "%s %d: downloaded  %s\n", __FILE__,__LINE__, media->download_data.url);
	    if (media->download_data.cookies)
		av_freep(&media->download_data.cookies);
	    av_opt_get(puc->priv_data, "cookies", 0, (uint8_t **) &media->download_data.cookies);
	    if (media->download_data.cookies && !strlen(media->download_data.cookies))
		av_freep(&media->download_data.cookies);
	    media->download_data.buffer = buffer;
	    media->download_data.buflen = buffer_size;
	}
	sem_post(media->download_data.to_caller);	// confirm download
	pthread_mutex_unlock(&media->download_data.mutex);
	if (puc)
	    ffurl_close(puc);
    }
    pthread_exit(NULL);
}

static void download_thread_start(AVFormatContext *s, HDSMedia *media)
{
#if defined(__APPLE__)
    char buf[40];
#endif
    pthread_mutex_init(&media->download_data.mutex, NULL);
#if defined(__APPLE__) // no unnamed semaphores on darwin
    snprintf(buf, sizeof(buf), "sem_to_thread%d", media->media_index);
    media->download_data.to_thread = sem_open(buf, O_CREAT, 0644, 0);
    snprintf(buf, sizeof(buf), "sem_to_caller%d", media->media_index);
    media->download_data.to_caller = sem_open(buf, O_CREAT, 0644, 0);
#else
    sem_init(&media->download_data._to_thread, 0, 0);
    sem_init(&media->download_data._to_caller, 0, 0);
    media->download_data.to_thread = &media->download_data._to_thread;
    media->download_data.to_caller = &media->download_data._to_caller;
#endif
    media->download_data.thread = 0;
    media->download_data.run = 1;
    media->download_data.abort = 0;
    media->download_data.url = NULL;
    media->download_data.buffer = NULL;
    media->download_data.buflen = 0;
    media->download_data.cookies = NULL;
    media->download_data.interrupt_callback = &s->interrupt_callback;
    media->download_data.download_abort_callback.callback = download_abort_callback_function;
    media->download_data.download_abort_callback.opaque = media;
    media->download_data.abort_callback = &media->download_data.download_abort_callback;
    if (pthread_create(&media->download_data.thread, NULL, download_thread, media))
	av_log(NULL, AV_LOG_ERROR, "hds: creating download thread failed\n");
}

static void download_thread_stop(HDSMedia *media)
{
    if (media->download_data.thread) {
#if defined(__APPLE__)
	char buf[40];
#endif
	media->download_data.run = 0;
	media->download_data.abort = 1;
	sem_post(media->download_data.to_thread);
	pthread_join(media->download_data.thread, NULL);
	media->download_data.thread = 0;
	if (media->download_data.url)
		av_freep(&media->download_data.url);
	if (media->download_data.cookies)
		av_freep(&media->download_data.cookies);
	if (media->download_data.buffer)
		av_freep(&media->download_data.buffer);
	media->download_data.buflen = 0;
#if defined(__APPLE__)
	sem_close(media->download_data.to_thread);
	sem_close(media->download_data.to_caller);
	snprintf(buf, sizeof(buf), "sem_to_thread%d", media->media_index);
	sem_unlink(buf);
	snprintf(buf, sizeof(buf), "sem_to_caller%d", media->media_index);
	sem_unlink(buf);
#else
	sem_destroy(&media->download_data._to_thread);
	sem_destroy(&media->download_data._to_caller);
#endif
	pthread_mutex_destroy(&media->download_data.mutex);
    }
}

static int download_fragment(AVFormatContext *s, HDSMedia *media, uint8_t **buffer_out, int *buffer_size_out)
{
    HDSContext *c = s->priv_data;
    char url[MAX_URL_SIZE];
    unsigned int segment, fragment;
    int ret;

    if((ret = get_segment_fragment(c->is_live, media, &segment, &fragment)) < 0)
	return ret;
    memset(url, 0x00, sizeof(url));
    if(!av_stristr(media->url, "?") && av_stristr(s->filename, "?"))
	construct_fragment_url(c->base_url, media->url, segment, fragment, av_stristr(s->filename, "?"), url, MAX_URL_SIZE);
    else
	construct_fragment_url(c->base_url, media->url, segment, fragment, "", url, MAX_URL_SIZE);

    pthread_mutex_lock(&media->download_data.mutex);

    if (!media->download_data.cookies)
	media->download_data.cookies = av_strdup(c->cookies);
    if (media->download_data.url && strcmp(media->download_data.url, url)) {
	// download in progress or finished, but not the wanted one. abort it.
	media->download_data.abort = 1;	// initiate abort
	pthread_mutex_unlock(&media->download_data.mutex);
	sem_wait(media->download_data.to_caller); // wait until current transfer has finished
	pthread_mutex_lock(&media->download_data.mutex);
	av_freep(&media->download_data.url);
    }
    if (!media->download_data.url) {
	// queue retrieval of wanted url
	media->download_data.url = av_strdup(url);
	sem_post(media->download_data.to_thread);	// initiate download
    }
    if (media->download_data.url && !strcmp(media->download_data.url, url)) {
	// download matches what we want
	pthread_mutex_unlock(&media->download_data.mutex);
	sem_wait(media->download_data.to_caller); // wait until finished
	pthread_mutex_lock(&media->download_data.mutex);
	av_freep(&media->download_data.url);
	if (media->download_data.buffer) {
	    *buffer_out = media->download_data.buffer;
	    *buffer_size_out = media->download_data.buflen;
	    media->download_data.buffer = NULL;
	    media->download_data.buflen = 0;
	    ret = 0;
	    media->nb_fragments_read++;
	    //initiate retrieval of next url
	    if(!get_segment_fragment(c->is_live, media, &segment, &fragment))
		memset(url, 0x00, sizeof(url));
		if(!av_stristr(media->url, "?") && av_stristr(s->filename, "?"))
		    construct_fragment_url(c->base_url, media->url, segment, fragment, av_stristr(s->filename, "?"), url, MAX_URL_SIZE);
		else
		    construct_fragment_url(c->base_url, media->url, segment, fragment, "", url, MAX_URL_SIZE);
		media->download_data.url = av_strdup(url);
		sem_post(media->download_data.to_thread);	// initiate download
	} else {
	    // finished but failed
	    ret = AVERROR(EIO);
	}
    }
    pthread_mutex_unlock(&media->download_data.mutex);
    return ret;
}

static int get_next_fragment(AVFormatContext *s, HDSMedia *media)
{
    F4FBox box;
    uint8_t *buffer = NULL;
    int buffer_size = 0, ret;

    if (ff_check_interrupt(media->download_data.interrupt_callback))
	return AVERROR(EIO);

    if((ret = download_fragment(s, media, &buffer, &buffer_size)) < 0)
	return ret;

    memset(&box, 0x00, sizeof(F4FBox));
    if((ret = ff_parse_f4f_box(buffer, buffer_size, &box)) < 0) {
	av_log(NULL, AV_LOG_ERROR, "hds Failed to parse bootstrap box, ret: %d\n", ret);
	av_free(buffer);
	ff_free_f4f_box(&box);
	return ret;
    }
    av_free(buffer);

    if((ret = ff_decode_flv_body(box.mdat.data, box.mdat.size, media->samples, &media->nb_samples)) < 0) {
	av_log(NULL, AV_LOG_ERROR, "hds Failed to decode FLV body, ret: %d\n", ret);
	ff_free_f4f_box(&box);
	return ret;
    }

    ff_free_f4f_box(&box);

    return 0;
}

static void read_next_sample(HDSMedia *media, AVPacket *pkt)
{
    FLVMediaSample *sample;

    sample = media->samples[media->sample_index];
    media->sample_index++;

    av_new_packet(pkt, sample->data_size);
    memcpy(pkt->data, sample->data, sample->data_size);

    pkt->dts = sample->dts;
    pkt->pts = sample->pts;

    if(sample->type == AVMEDIA_TYPE_VIDEO && media->video_stream)
	pkt->stream_index = media->video_stream->index;
    else if(sample->type == AVMEDIA_TYPE_AUDIO && media->audio_stream)
	pkt->stream_index = media->audio_stream->index;
}

static void clear_samples(HDSMedia *media)
{
    FLVMediaSample *sample;
    int i;

    for(i = 0; i < media->nb_samples; i++) {
	sample = media->samples[i];
	av_freep(&sample->data);
	av_freep(&sample);
	media->samples[i] = NULL;
    }

    media->nb_samples = 0;
    media->sample_index = 0;
}

static int get_next_packet(AVFormatContext *s, HDSMedia *media, AVPacket *pkt)
{
    HDSContext *c = s->priv_data;
    int ret;

    if (c->is_live && !media->nb_fragments_read) {
	int64_t ts = media->bootstrap_info->box.abst.current_media_time;
	media->nb_fragments_read = get_fragment_offset(media, ts);
	if (media->nb_fragments_read > 1)
		media->nb_fragments_read--;
    }

    if (c->seek_timestamp != AV_NOPTS_VALUE) {
	int64_t ts = c->seek_timestamp;
	c->seek_timestamp = AV_NOPTS_VALUE;
	media->nb_fragments_read = get_fragment_offset(media, ts);
	clear_samples(media);
    }

    if(media->nb_samples == 0) {
	if((ret = get_next_fragment(s, media)) < 0) {
	    return ret;
	}
    }

    if(media->nb_samples > 0) {
	read_next_sample(media, pkt);
    }

    if(media->sample_index >= media->nb_samples) {
	clear_samples(media);
    }

    return 0;
}

static int hds_read_packet(AVFormatContext *s, AVPacket *pkt)
{
    HDSContext *c = s->priv_data;
    HDSMedia *media = NULL;
    int i, ret;

    for (i = c->last_media_index + 1; !media && i < c->nb_media; i++) {
	media = c->media[i];
	if ((!media->video_stream || (media->video_stream->discard == AVDISCARD_ALL))
	 && (!media->audio_stream || (media->audio_stream->discard == AVDISCARD_ALL)))
	    media = NULL;
    }
    for (i = 0; !media && i < c->nb_media; i++) {
	media = c->media[i];
	if ((!media->video_stream || (media->video_stream->discard == AVDISCARD_ALL))
	 && (!media->audio_stream || (media->audio_stream->discard == AVDISCARD_ALL)))
	    media = NULL;
    }
    c->last_media_index = i;

    if (media && !media->bootstrap_info && c->nb_bootstraps)
        media->bootstrap_info = c->bootstrap_info[0];

    if (!media || !media->bootstrap_info) {
	av_log(NULL, AV_LOG_ERROR, "hds Failed to find valid stream\n");
	return AVERROR(EIO);
    }

    if((ret = get_next_packet(s, media, pkt)) < 0) {
	if(ret != AVERROR_EOF)
	    av_log(NULL, AV_LOG_ERROR, "hds Failed to get next packet, ret: %d\n", ret);
	return ret;
    }

    return 0;
}

static int hds_close(AVFormatContext *s)
{
    HDSContext *c = s->priv_data;
    HDSBootstrapInfo *bootstrap_info;
    HDSMedia *media;
    int i;

    for(i = 0; i < c->nb_media; i++)
	download_thread_stop(c->media[i]);

    for(i = 0; i < c->nb_bootstraps; i++) {
	bootstrap_info = c->bootstrap_info[i];

	ff_free_f4f_box(&bootstrap_info->box);
	av_freep(&bootstrap_info);
    }

    for(i = 0; i < c->nb_media; i++) {
	media = c->media[i];

	if (media->download_data.buffer)
	    av_freep(&media->download_data.buffer);

	clear_samples(media);
	av_freep(&media);
    }

    if(c->cookies)
	av_freep(&c->cookies);

    memset(c, 0x00, sizeof(HDSContext));

    return 0;
}

static int hds_probe(AVProbeData *p)
{
    if(p->filename && av_stristr(p->filename, ".f4m"))
	return AVPROBE_SCORE_MAX;
    return 0;
}

static int hds_read_seek(AVFormatContext *s, int stream_index,
			       int64_t timestamp, int flags)
{
    HDSContext *c = s->priv_data;

    if (flags & AVSEEK_FLAG_BYTE)
	return AVERROR(ENOSYS);
    if (s->duration < c->seek_timestamp) {
	c->seek_timestamp = AV_NOPTS_VALUE;
	return AVERROR(EIO);
    }
    c->seek_timestamp = timestamp;

    if (c->is_live) {
	int i;
	for(i = 0; i < c->nb_media; i++)
	    c->media[i]->nb_fragments_read = 0;
	c->seek_timestamp = AV_NOPTS_VALUE;
	return 0;
    }

    return 0;
}

AVInputFormat ff_hds_demuxer = {
    .name	   = "hds",
    .long_name      = NULL_IF_CONFIG_SMALL("Adobe HTTP Dynamic Streaming Demuxer"),
    .priv_data_size = sizeof(HDSContext),
    .read_probe     = hds_probe,
    .read_header    = hds_read_header,
    .read_packet    = hds_read_packet,
    .read_close     = hds_close,
    .read_seek      = hds_read_seek,
};
