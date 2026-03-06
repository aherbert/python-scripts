#!/usr/bin/env python3
"""Exports a plate."""

import argparse
import os
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import numpy as np
from omero.gateway import BlitzGateway
from omero.rtypes import unwrap
from tifffile import imwrite

import omero


def main():
    parser = argparse.ArgumentParser(description="Exports a plate.")
    parser.add_argument(
        "plate_id",
        type=int,
        help="Plate ID",
    )
    _ = parser.add_argument(
        "--user",
        help="OMERO username",
    )
    _ = parser.add_argument(
        "--password",
        help="OMERO password",
    )
    _ = parser.add_argument(
        "--host",
        help="OMERO host",
    )
    _ = parser.add_argument(
        "--numpy",
        default=False,
        action=argparse.BooleanOptionalAction,
        help="Export as numpy",
    )
    _ = parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Concurrent workers",
    )
    args = parser.parse_args()

    conn = BlitzGateway(args.user, args.password, host=args.host)
    conn.connect()
    if not conn.isConnected():
        raise RuntimeError(f"Failed to connect to OMERO at {args.host}")
    conn.c.enableKeepAlive(60)

    query_service = conn.getQueryService()
    params = omero.sys.ParametersI()
    params.addLong("plate_id", args.plate_id)
    query = """
        select i.id, pi.id, pi.sizeT, pi.sizeC, pi.sizeZ, pi.sizeY, pi.sizeX, pt.value
        from Plate as p
          left join p.wells as w
          left join w.wellSamples as ws
          left join ws.image as i
          left join i.pixels as pi
          left join pi.pixelsType as pt
        where p.id = :plate_id
        order by w.id
    """
    results = query_service.projection(query, params, conn.SERVICE_OPTS)
    if not results:
        raise Exception(f"Invalid plate: {args.plate_id}")
    images = []
    for row_data in results:
        images.append(
            (
                unwrap(row_data[0]),
                unwrap(row_data[1]),
                unwrap(row_data[2]),
                unwrap(row_data[3]),
                unwrap(row_data[4]),
                unwrap(row_data[5]),
                unwrap(row_data[6]),
                unwrap(row_data[7]),
            )
        )

    # No longer required
    conn.close()

    dir = f"plate{args.plate_id}"
    os.makedirs(dir, exist_ok=True)

    # Process in batches
    batches = [[] for _ in range(args.workers)]
    for i, image in enumerate(images):
        batches[i % args.workers].append(image)

    # Download a batch
    def download_batch(batch):
        conn = BlitzGateway(args.user, args.password, host=args.host)
        conn.connect()
        if not conn.isConnected():
            raise RuntimeError(f"Failed to connect to OMERO at {args.host}")
        store = None
        try:
            conn.c.enableKeepAlive(60)
            store = conn.c.sf.createRawPixelsStore()

            for (
                image_id,
                pixels_id,
                sizeT,
                sizeC,
                sizeZ,
                sizeY,
                sizeX,
                pixels_type,
            ) in batch:
                print(f"Image {image_id}: z={sizeZ}, c={sizeC}, t={sizeT}")

                image = conn.getObject("Image", image_id)

                zctList = []
                for z in range(sizeZ):
                    for c in range(sizeC):
                        for t in range(sizeT):
                            zctList.append((z, c, t))

                planes = image.getPrimaryPixels().getPlanes(zctList)

                for pixels, (z, c, t) in zip(planes, zctList):
                    if args.numpy:
                        np.save(
                            os.path.join(dir, f"s{image_id}_z{z}_c{c}_t{t}.npy"), pixels
                        )
                    else:
                        imwrite(
                            os.path.join(dir, f"s{image_id}_z{z}_c{c}_t{t}.tiff"),
                            pixels,
                        )
        finally:
            if store is not None:
                store.close()
            conn.close()

    if args.workers == 1:
        download_batch(batches[0])
    else:
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            futures = [
                executor.submit(
                    download_batch,
                    batch,
                )
                for batch in batches
            ]
            for f in futures:
                exc = f.exception()
                if exc is not None:
                    raise exc

    conn.close()


if __name__ == "__main__":
    main()
