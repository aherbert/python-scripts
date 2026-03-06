#!/usr/bin/env python3
"""Exports a plate."""

import argparse
import os
from typing import Any

import numpy as np
from omero.cli import cli_login
from omero.gateway import BlitzGateway
from tifffile import imwrite

# Mapping from OMERO pixel type names to big-endian numpy dtypes.
# OMERO transmits raw pixel bytes in network (big-endian) byte order.
_OMERO_PIXEL_DTYPES: dict[str, np.dtype[Any]] = {
    "uint8": np.dtype(">u1"),
    "uint16": np.dtype(">u2"),
    "int8": np.dtype(">i1"),
    "int16": np.dtype(">i2"),
    "int32": np.dtype(">i4"),
    "float": np.dtype(">f4"),
    "double": np.dtype(">f8"),
}


def main():
    parser = argparse.ArgumentParser(description="Exports a plate.")
    parser.add_argument(
        "plate_id",
        type=int,
        help="Plate ID",
    )
    _ = parser.add_argument(
        "--numpy",
        default=False,
        action=argparse.BooleanOptionalAction,
        help="Export as numpy",
    )
    args = parser.parse_args()

    with cli_login() as cli:
        conn = BlitzGateway(client_obj=cli._client)
        plate = conn.getObject("Plate", args.plate_id)
        if plate is None:
            raise Exception(f"Invalid plate: {args.plate_id}")

        store = conn.c.sf.createRawPixelsStore()

        dir = f"plate{args.plate_id}"
        os.makedirs(dir, exist_ok=True)

        wells = list(plate.listChildren())
        for well in wells:
            well_dir = os.path.join(dir, well.getWellPos())
            os.makedirs(well_dir, exist_ok=True)

            well_samples = list(well.listChildren())
            print(f"{well.getWellPos()}, # samples= {len(well_samples)}")

            for i, well_sample in enumerate(well_samples):
                image = well_sample.getImage()
                sizeZ = image.getSizeZ()
                sizeC = image.getSizeC()
                sizeT = image.getSizeT()
                print(f"Sample {i}: z={sizeZ}, c={sizeC}, t={sizeT}")

                pixels = image.getPrimaryPixels()
                pixel_type = pixels.getPixelsType().getValue()
                dt_be = _OMERO_PIXEL_DTYPES.get(pixel_type)
                if dt_be is None:
                    raise ValueError(f"Unsupported OMERO pixel type: {pixel_type}")
                store.setPixelsId(pixels.getId(), False)
                sizeY = image.getSizeY()
                sizeX = image.getSizeX()

                for t in range(sizeT):
                    raw_bytes = store.getTimepoint(t)
                    arr = np.frombuffer(raw_bytes, dtype=dt_be).reshape(
                        sizeC, sizeZ, sizeY, sizeX
                    )
                    if args.numpy:
                        np.save(os.path.join(well_dir, f"s{i}_t{t}.npy"), arr)
                    else:
                        imwrite(os.path.join(well_dir, f"s{i}_t{t}.tiff"), arr)

        store.close()


if __name__ == "__main__":
    main()
