/*
 * copyright (c) 2010 Google Inc.
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
#include "bsf.h"

#define RCV_STREAM_HEADER_SIZE 36
#define RCV_PICTURE_HEADER_SIZE 8

typedef struct ASFTORCVBSFContext {
    int frames;
} ASFTORCVBSFContext;

static int asftorcv_filter(AVBSFContext *ctx, AVPacket *out){
    ASFTORCVBSFContext* bsfctx = ctx->priv_data;
    AVPacket *in;
    int keyframe = 0;
    int ret = 0;
    uint8_t* bs = NULL;

    ret = ff_bsf_get_packet(ctx, &in);
    if (ret < 0)
        return ret;

    keyframe = in->flags & AV_PKT_FLAG_KEY;

    if (!bsfctx->frames) {
        // Write the header if this is the first frame.
        out->data = av_malloc(RCV_STREAM_HEADER_SIZE + RCV_PICTURE_HEADER_SIZE + in->size);
        out->size = RCV_STREAM_HEADER_SIZE + RCV_PICTURE_HEADER_SIZE + in->size;
        bs = out->data;

        // The following structure of stream header comes from libavformat/vc1testenc.c.
        bytestream_put_le24(&bs, 0);  // Frame count. 0 for streaming.
        bytestream_put_byte(&bs, 0xC5);
        bytestream_put_le32(&bs, 4);  // 4 bytes of extra data.
        bytestream_put_byte(&bs, ctx->par_in->extradata[0]);
        bytestream_put_byte(&bs, ctx->par_in->extradata[1]);
        bytestream_put_byte(&bs, ctx->par_in->extradata[2]);
        bytestream_put_byte(&bs, ctx->par_in->extradata[3]);
        bytestream_put_le32(&bs, ctx->par_in->height);
        bytestream_put_le32(&bs, ctx->par_in->width);
        bytestream_put_le32(&bs, 0xC);
        bytestream_put_le24(&bs, 0);  // hrd_buffer
        bytestream_put_byte(&bs, 0x80);  // level|cbr|res1
        bytestream_put_le32(&bs, 0);  // hrd_rate

        // The following LE32 describes the frame rate. Since we don't care so fill
        // it with 0xFFFFFFFF which means variable framerate.
        // See: libavformat/vc1testenc.c
        bytestream_put_le32(&bs, 0xFFFFFFFF);
    } else {
        out->data = av_malloc(RCV_PICTURE_HEADER_SIZE + in->size);
        out->size = RCV_PICTURE_HEADER_SIZE + in->size;
        bs = out->data;
    }

    // Write the picture header.
    bytestream_put_le32(&bs, in->size | (keyframe ? 0x80000000 : 0));

    //  The following LE32 describes the pts. Since we don't care so fill it with 0.
    bytestream_put_le32(&bs, 0);
    memcpy(bs, in->data, in->size);

    ++bsfctx->frames;
    return 0;
}

static const enum AVCodecID codec_ids[] = {
    AV_CODEC_ID_WMV3, AV_CODEC_ID_NONE,
};

AVBitStreamFilter ff_vc1_asftorcv_bsf = {
    .name 	    = "vc1_asftorcv",
    .priv_data_size = sizeof(ASFTORCVBSFContext),
    .filter         = asftorcv_filter,
};
