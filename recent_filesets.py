#!/usr/bin/env python3
"""Reports recent imports."""

import sys
from datetime import datetime
import argparse

import omero
import omero.clients
from omero.rtypes import unwrap
from omero.cli import cli_login
from omero.gateway import BlitzGateway


def main():
    starttime = datetime.today()
    starttime = datetime(starttime.year, starttime.month - 1, starttime.day).strftime(
        "%Y-%m-%d"
    )
    parser = argparse.ArgumentParser(description="Reports recent imports.")
    parser.add_argument(
        "--start",
        default=starttime,
        help="Start date for HQL query (default: %(default)s)",
    )
    parser.add_argument(
        "--example",
        default=True,
        action=argparse.BooleanOptionalAction,
        help="Print an example file path (default: %(default)s)",
    )
    parser.add_argument(
        "-n",
        "--limit",
        dest="limit",
        type=int,
        default=1000,
        help="Page limit (default: %(default)s)",
    )
    args = parser.parse_args()

    with cli_login() as cli:
        conn = BlitzGateway(client_obj=cli._client)
        query_service = conn.getQueryService()

        # https://omero.readthedocs.io/en/stable/developers/Model/EveryObject.html#session
        # https://omero.readthedocs.io/en/stable/developers/Model/EveryObject.html#experimenter
        params = omero.sys.ParametersI()
        offset = 0
        limit = 1000
        params.page(offset, args.limit)

        # # No apparent job for imports!
        # # Job - many .log files; some stdout; one python script
        # # UploadJob - many .log files; no tiff files
        # # ImportJob (deprecated) - empty
        # # MetadataImportJob - empty
        # # ParseJob - empty
        # # PixelDataJob - empty
        # # IndexingJob - empty
        # query = f"""select j from Job as j
        #     left outer join fetch j.originalFileLinks as link
        #     join fetch link.child
        #     where j.submitted > '{args.start}'
        #     order by j.submitted desc
        # """
        # results = query_service.findAllByQuery(query, params, None)
        # for job in results:
        #     print(
        #         unwrap(job.username),
        #         str(datetime.fromtimestamp(job.submitted.val / 1000)),
        #         "Files",
        #         sum(1 for _ in job.iterateOriginalFileLinks()),
        #         next(job.iterateOriginalFileLinks()).child.name.val,
        #     )

        # # This query hits the ICE message size limit (256000000 bytes) if too many results are returned (e.g. 80)
        # # The size is due to the fetch of the used files (and possibly all the other linked information)
        # query = f"""select f from Fileset as f
        #     left outer join fetch f.usedFiles
        #     join fetch f.details as d
        #     join fetch d.owner
        #     join fetch d.creationEvent
        #     where f.details.creationEvent.time > '{args.start}'
        #     order by f.details.creationEvent.time desc
        # """
        # results = query_service.findAllByQuery(query, params, None)
        # for f in results:
        #     print(
        #         unwrap(f.details.owner._omeName),
        #         str(
        #             datetime.fromtimestamp(unwrap(f.details.creationEvent.time) / 1000)
        #         ),
        #         "Files",
        #         len(f._usedFilesSeq),
        #     )
        #     o = conn.getObject("OriginalFile", f._usedFilesSeq[0]._originalFile.id._val)
        #     print("  ", o.path, o.name)

        # More managable to use smaller queries
        query = f"""select f from Fileset as f
            join fetch f.details as d
            join fetch d.owner
            join fetch d.creationEvent
            where d.creationEvent.time > '{args.start}'
            order by d.creationEvent.time desc
        """
        results = query_service.findAllByQuery(query, params, None)
        for f in results:
            # Count the number of file. This is fast.
            # https://docs.openmicroscopy.org/omero-blitz/5.7.3/slice2html/omero/api/IQuery.html#projection
            params = omero.sys.ParametersI()
            params.page(offset, limit)
            query = f"select count(*) from FilesetEntry as fse where fse.fileset.id={f.id._val}"
            proj = query_service.projection(query, params, None)
            # Each element of the outer sequence is one row in the return value.
            # Each element of the inner sequence is one column specified in the HQL.
            count = unwrap(proj[0][0])

            print(
                unwrap(f.details.owner._omeName),
                str(
                    datetime.fromtimestamp(unwrap(f.details.creationEvent.time) / 1000)
                ),
                "Files",
                count,
            )
            if args.example:
                params = omero.sys.ParametersI()
                params.page(0, 1)

                # Adapted from FilesetWrapper.listFiles to only get 1 file
                # This is slow as it obtains a full object and we then get an original file in a second query
                # which returns more object details than required.
                # query = f"select fse from FilesetEntry as fse where fse.fileset.id={f.id._val}"
                # usedFilesSeq = query_service.findByQuery(query, params, None)
                # o = conn.getObject("OriginalFile", usedFilesSeq._originalFile.id._val)
                # print("  e.g.", unwrap(o.path), unwrap(o.name))

                # Get the attributes we require directly
                query = f"select o.path, o.name from FilesetEntry as fse join fse.originalFile o where fse.fileset.id={f.id._val}"
                proj = query_service.projection(query, params, None)
                print("  e.g.", unwrap(proj[0][0]), unwrap(proj[0][1]))


if __name__ == "__main__":
    main()
