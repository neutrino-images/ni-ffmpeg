/*
 * copyright (c) 2010 Google Inc.
 * copyright (c) 2013 CoolStream International Ltd.
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

#include "avcodec.h"
#include "bytestream.h"
#include "vc1.h"

// An arbitrary limit in bytes greater than the current bytes used.
#define MAX_SEQ_HEADER_SIZE 50

typedef struct ASFTOANNEXGBSFContext {
    int frames;
    uint8_t *seq_header;
    int seq_header_size;
    uint8_t *ep_header;
    int ep_header_size;
} ASFTOANNEXGBSFContext;

static int find_codec_data(AVCodecContext *avctx, ASFTOANNEXGBSFContext *ctx, uint8_t *data, int data_size, int keyframe) {
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

static int parse_extradata(AVCodecContext *avctx, ASFTOANNEXGBSFContext *ctx, uint8_t *extradata, int extradata_size) {
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
        av_log(avctx, AV_LOG_ERROR, "Incomplete extradata\n");
        return -1;
    }
    return 0;
}

static int asftoannexg_filter(AVBitStreamFilterContext *bsfc, AVCodecContext *avctx, const char *args,
                              uint8_t **poutbuf, int *poutbuf_size,
                              const uint8_t *buf, int buf_size, int keyframe){
    ASFTOANNEXGBSFContext* ctx = (ASFTOANNEXGBSFContext*)bsfc->priv_data;

    if (avctx->codec_id != CODEC_ID_VC1) {
        av_log(avctx, AV_LOG_ERROR, "Only VC1 Advanced profile is accepted!\n");
        return -1;
    }

    //if (!ctx->frames)
    //    av_hex_dump(stdout, avctx->extradata, avctx->extradata_size);

    /* Check if the main stream contains the codes already */
    if(buf_size >= 1 && !find_codec_data(avctx, ctx, buf, buf_size, keyframe)) {
        av_log(avctx, AV_LOG_INFO, "Nothing to do: %i\n");
        *poutbuf = buf;
        *poutbuf_size = buf_size;
        return 0;
    }

    if(!avctx->extradata || avctx->extradata_size < 16) {
        av_log(avctx, AV_LOG_INFO, "Extradata size too small: %i\n", avctx->extradata_size);
        *poutbuf = buf;
        *poutbuf_size = buf_size;
        return 0;
    }

    if (!ctx->frames && parse_extradata(avctx, ctx, avctx->extradata + 1, avctx->extradata_size - 1) < 0) {
        av_log(avctx, AV_LOG_ERROR, "Cannot parse extra data!\n");
        return -1;
    }

    uint8_t* bs;
    if (keyframe) {
        // If this is the keyframe, need to put sequence header and entry point header.
        *poutbuf_size = ctx->seq_header_size + ctx->ep_header_size + 4 + buf_size;
        *poutbuf = av_malloc(*poutbuf_size);
        bs = *poutbuf;

        memcpy(bs, ctx->seq_header, ctx->seq_header_size);
        bs += ctx->seq_header_size;
        memcpy(bs, ctx->ep_header, ctx->ep_header_size);
        bs += ctx->ep_header_size;
    } else {
        *poutbuf_size = 4 + buf_size;
        *poutbuf = av_malloc(*poutbuf_size);
        bs = *poutbuf;
    }

    // Put the frame start code and frame data.
    bytestream_put_be32(&bs, VC1_CODE_FRAME);
    memcpy(bs, buf, buf_size);
    ++ctx->frames;
    return 1;
}

static void asftoannexg_close(AVBitStreamFilterContext *bsfc) {
    ASFTOANNEXGBSFContext *ctx = bsfc->priv_data;
    av_freep(&ctx->seq_header);
    av_freep(&ctx->ep_header);
}

AVBitStreamFilter ff_vc1_asftoannexg_bsf = {
    "vc1_asftoannexg",
    sizeof(ASFTOANNEXGBSFContext),
    asftoannexg_filter,
    asftoannexg_close,
};
