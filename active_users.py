#!/usr/bin/env python3
"""Reports active users following a start date."""

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
    starttime = datetime(starttime.year - 1, starttime.month, starttime.day).strftime(
        "%Y-%m-%d"
    )
    parser = argparse.ArgumentParser(
        description="Reports active users following a start date."
    )
    parser.add_argument(
        "--start",
        default=starttime,
        help="Start date for HQL query (default: %(default)s)",
    )
    args = parser.parse_args()

    with cli_login() as cli:
        conn = BlitzGateway(client_obj=cli._client)
        query_service = conn.getQueryService()

        # https://omero.readthedocs.io/en/stable/developers/Model/EveryObject.html#session
        # https://omero.readthedocs.io/en/stable/developers/Model/EveryObject.html#experimenter
        params = omero.sys.ParametersI()
        # offset = 0
        # limit = 1000
        # params.page(offset, limit)

        # HQL is a bit limited here. Ideally we would want to select the top 1 experimenter and started date
        # when grouped by experimenter and ordered by started date descending, i.e. the most recent session
        # for each user.
        # This orders by the started date but cannot return the actual date!
        query = f"""select e from Session as s
            left join s.owner as e
            where s.started > '{args.start}'
            group by e
            order by max(s.started) desc
        """
        results = query_service.findAllByQuery(query, params, None)
        for owner in results:
            omeName = unwrap(owner.omeName)
            if omeName in ["root", "guest"]:
                continue
            print(
                unwrap(owner.id),
                omeName,
                unwrap(owner.email),
                unwrap(owner.firstName),
                unwrap(owner.lastName),
            )


if __name__ == "__main__":
    main()
