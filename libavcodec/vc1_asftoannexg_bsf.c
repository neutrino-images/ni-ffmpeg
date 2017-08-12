/*
 * copyright (c) 2010 Google Inc.
 * copyright (c) 2013 CoolStream International Ltd.
 * copyright (c) 2017 Jacek Jendrzej port to 3.x
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

#include "avcodec.h"
#include "bytestream.h"
#include "vc1.h"
#include "bsf.h"

// An arbitrary limit in bytes greater than the current bytes used.
#define MAX_SEQ_HEADER_SIZE 50

typedef struct ASFTOANNEXGBSFContext {
    int frames;
    uint8_t *seq_header;
    int seq_header_size;
    uint8_t *ep_header;
    int ep_header_size;
} ASFTOANNEXGBSFContext;

static int find_codec_data(ASFTOANNEXGBSFContext *ctx, uint8_t *data, int data_size, int keyframe) {
    const uint8_t *start = data;
    const uint8_t *end = data + data_size;
    const uint8_t *next;
    int size;
    int has_seq_header = 0;
    int has_ep_header = 0;
    int has_frame_header = 0;

    start = find_next_marker(start, end);
    next = start;
    for(; next < end; start = next){
        next = find_next_marker(start + 4, end);
        size = next - start;
        if(size <= 0) continue;
        switch(AV_RB32(start)){
        case VC1_CODE_SEQHDR:
            has_seq_header = 1;
            break;
        case VC1_CODE_ENTRYPOINT:
            has_ep_header = 1;
            break;
        case VC1_CODE_FRAME:
            has_frame_header = 1;
            break;
        default:
            break;
        }
    }

    if((has_seq_header && has_ep_header && has_frame_header && keyframe) ||
       (!has_seq_header && !has_ep_header && has_frame_header) ) return 0;

    return -1;
}

static int parse_extradata(ASFTOANNEXGBSFContext *ctx, uint8_t *extradata, int extradata_size) {
    const uint8_t *start = extradata;
    const uint8_t *end = extradata + extradata_size;
    const uint8_t *next;
    int size;

    start = find_next_marker(start, end);
    next = start;
    for(; next < end; start = next){
        next = find_next_marker(start + 4, end);
        size = next - start;
        if(size <= 0) continue;
        switch(AV_RB32(start)){
        case VC1_CODE_SEQHDR:
            ctx->seq_header = av_mallocz(size);
            ctx->seq_header_size = size;
            memcpy(ctx->seq_header, start, size);
            break;
        case VC1_CODE_ENTRYPOINT:
            ctx->ep_header = av_malloc(size);
            ctx->ep_header_size = size;
            memcpy(ctx->ep_header, start, size);
            break;
        default:
            break;
        }
    }

    if(!ctx->seq_header || !ctx->ep_header) {
        av_log(NULL, AV_LOG_ERROR, "Incomplete extradata\n");
        return -1;
    }
    return 0;
}

static int asftoannexg_filter(AVBSFContext *ctx, AVPacket *out)
{
    ASFTOANNEXGBSFContext* bsfctx = ctx->priv_data;
    AVPacket *in;
    int keyframe = 0;
    int ret = 0;
    uint8_t* bs = NULL;

    ret = ff_bsf_get_packet(ctx, &in);
    if (ret < 0)
        return ret;

    keyframe = in->flags & AV_PKT_FLAG_KEY;
    if(in->size >= 1 && !find_codec_data(bsfctx, in->data, in->size, keyframe)) {
//         av_log(NULL, AV_LOG_INFO, "Nothing to do: %i\n",in->size);
        out->data = in->data;
        out->size = in->size;
        return 0;
    }

    if(!ctx->par_in->extradata || ctx->par_in->extradata_size < 16) {
        av_log(NULL, AV_LOG_INFO, "Extradata size too small: %i\n", ctx->par_in->extradata_size);
        out->data = in->data;
        out->size = in->size;
        return 0;
    }

    if (!bsfctx->frames && parse_extradata(bsfctx, ctx->par_in->extradata , ctx->par_in->extradata_size ) < 0) {
        av_log(NULL, AV_LOG_ERROR, "Cannot parse extra data!\n");
        return -1;
    }

    if (keyframe) {
        // If this is the keyframe, need to put sequence header and entry point header.
        out->size = bsfctx->seq_header_size + bsfctx->ep_header_size + 4 + in->size;
        out->data = av_malloc(out->size);
        bs = out->data;

        memcpy(bs, bsfctx->seq_header, bsfctx->seq_header_size);
        bs += bsfctx->seq_header_size;
        memcpy(bs, bsfctx->ep_header, bsfctx->ep_header_size);
        bs += bsfctx->ep_header_size;
    } else {
        out->size = 4 + in->size;
        out->data = av_malloc(out->size);
        bs = out->data;
    }

    // Put the frame start code and frame data.
    bytestream_put_be32(&bs, VC1_CODE_FRAME);
    memcpy(bs, in->data, in->size);
    ++bsfctx->frames;
    return 1;

 }

static void asftoannexg_close(AVBSFContext *bsfc) {
    ASFTOANNEXGBSFContext *bsfctx = bsfc->priv_data;
    av_freep(&bsfctx->seq_header);
    av_freep(&bsfctx->ep_header);
}

static const enum AVCodecID codec_ids[] = {
    AV_CODEC_ID_VC1, AV_CODEC_ID_NONE,
};

AVBitStreamFilter ff_vc1_asftoannexg_bsf = {
    .name           = "vc1_asftoannexg",
    .priv_data_size = sizeof(ASFTOANNEXGBSFContext),
    .filter         = asftoannexg_filter,
    .close          = asftoannexg_close,
    .codec_ids      = codec_ids,
};
