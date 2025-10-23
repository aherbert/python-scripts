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
    parser.add_argument(
        "--sep",
        default="\t",
        help="Separator for output (defaults to tab)",
    )
    args = parser.parse_args()

    with cli_login() as cli:
        conn = BlitzGateway(client_obj=cli._client)
        query_service = conn.getQueryService()

        print("ID", "OME Name", "email", "Name", "Count", "Latest", sep=args.sep)

        # https://omero.readthedocs.io/en/stable/developers/Model/EveryObject.html#session
        # https://omero.readthedocs.io/en/stable/developers/Model/EveryObject.html#experimenter
        params = omero.sys.ParametersI()

        # Select the most recent session for each user, and count the sessions.
        query = f"""select e.id, e.omeName, e.email, e.firstName, e.lastName, count(e.id), max(s.started) from Session as s
            left join s.owner as e
            where s.started > '{args.start}'
            group by e
            order by max(s.started) desc
        """
        results = query_service.projection(query, params, None)
        for id, omeName, email, firstName, lastName, count, started in results:
            omeName = unwrap(omeName)
            if omeName in ["root", "guest"]:
                continue
            print(
                unwrap(id),
                omeName,
                unwrap(email),
                " ".join([unwrap(firstName), unwrap(lastName)]),
                unwrap(count),
                str(datetime.fromtimestamp(unwrap(started) / 1000)),
                sep=args.sep,
            )


if __name__ == "__main__":
    main()
