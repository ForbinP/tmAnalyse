from tmDumpCore import write_from_cursor


# noinspection PyUnusedLocal
def build_query_string(table, normal_columns, text_columns, filter_clause=''):
    """
    Return a string containing a SELECT statement to extract the requested columns
    from the specified `table`, subject to an optional `filter_clause`.

    (NB: Telemetry's textual data is handled specially, with indices into the global
    'tmexpandedtext' table, so any `text_columns` have to be specified separately
    so that we know when to perform this indirect lookup.)

    Doctest example follows (with output split over multiple lines, for clarity)
    Run it via 'nosetests --with-doctest <filename>'

    >>> build_query_string('myTable',
    ...                    ['col1', 'col2'],
    ...                    ['textCol1', 'textCol2'],
    ...                    'filter')
    ... # doctest: +NORMALIZE_WHITESPACE
    'SELECT col1, col2,
            text0.content AS textCol1_text,
            text1.content AS textCol2_text
    FROM myTable
    LEFT JOIN tmexpandedtext AS text0 ON text0.id = textCol1
    LEFT JOIN tmexpandedtext AS text1 ON text1.id = textCol2
    filter'
    """
    normal_part = ', '.join(normal_columns)
    text_part = ', '.join(
        'text{i}.content AS {name}_text'.format(i=i, name=name)
        for i, name in enumerate(text_columns)
    )
    join_clause = ' '.join(
        'LEFT JOIN tmexpandedtext AS text{i} ON text{i}.id = {col}'.
        format(i=i, col=column)
        for i, column in enumerate(text_columns)
    )
    return ('SELECT {normal_part}, {text_part} FROM {table} '
            '{join_clause} '
            '{filter_clause}'.
            format(**vars()))


def get_clocks_per_second_from_rows(rows, ticks_per_second):
    """
    Return an approximate `clocks_per_second` value, by calibrating the raw
    (unscaled) 'tsc' values against the semi-reliable 'tick' values which,
    in turn, are calibrated to some values of `ticks_per_second`.

    NB: We just infers this from 2 adjacent ticks in the middle of our sample.
    This approach is certainly crude, but it is also fast and simple.

    Each entry in `rows` is a pair in the form: (tick, tsc)

    >>> rows = ((1,10), (2,20), (3,30), (4,40), (5,50))
    >>> get_clocks_per_second_from_rows(rows, ticks_per_second=100)
    1000.0
    """

    mid = len(rows) / 2
    a = rows[mid]
    b = rows[mid + 1]

    dtick = b[0] - a[0]
    dtsc = b[1] - a[1]

    clocks_per_tick = float(dtsc) / float(dtick)
    clocks_per_second = clocks_per_tick * ticks_per_second

    return clocks_per_second


def get_clocks_per_second(conn):
    """
    get_clocks_per_second

    Each platform has unique timing characteristics.  To somewhat deal with this
    Telemetry samples both the clock cycle counter and 'tick' time whenever
    tmTick is called.  This information is then used to create a conversion from
    event clock cycle samples to wall time.

    This function pulls the relevant ticks_per_second (reported once at startup
    during capture) from the session info table, then grabs two centrally located
    ticks to determine the conversion factor.  This isn't ideal, but more precise
    approximations are significantly more complex because they'd have to be
    evaluated at each event time by examining the surrounding ticks.
    """
    ticks_per_second = conn.execute(
        'SELECT ticks_per_second FROM tmsessioninfo').fetchone()[0]

    rows = conn.execute(
        'SELECT tick, tsc FROM tmticks ORDER BY tick').fetchall()

    return get_clocks_per_second_from_rows(rows, ticks_per_second)


def write_zones(conn, cur, writer):
    clocks_per_second = get_clocks_per_second(conn)

    writer.writerow(['name', 'thread id', 'start time', 'duration (ms)', 'depth'])
    for row in cur:
        writer.writerow([
            row['fullname_id_text'],
            row['thread_id'],
            row['start_tsc'] / clocks_per_second,  # start time (sec)
            ((row['end_tsc'] - row['start_tsc']) *
             1000 / clocks_per_second),  # duration (ms)
            row['depth']
        ])


def dump_zones(conn, csv_writer):
    """
    Dump the tmzones data as CSV

    :param conn: connection object to the DB we are using
    :param csv_writer: output goes to this csvwriter object
    """
    query = build_query_string(
        table='tmzones',

        normal_columns=[
            'start_tsc', 'end_tsc', 'depth', 'line', 'flags',
            'thread_id', 'process_id', 'lock_ptr'],

        text_columns=[
            'fullname_id', 'filename_id', 'path_id']
    )
    cur = conn.execute(query)

    write_zones(conn, cur, csv_writer)


def dump_zone_totals(conn, csv_writer):
    """
    Totalise the time spent in each zone.

    (And convert the fullname_id into its corresponding text.)
    """
    cur = conn.execute("""
        SELECT
              SUM(end_tsc - start_tsc) AS time,
              text.content AS fullname
        FROM  tmzones
        JOIN  tmexpandedtext AS text
        ON    tmzones.fullname_id == text.id
        GROUP BY fullname
        ORDER BY time DESC
    """)

    write_from_cursor(cur, csv_writer)


def dump_zone_totals_exclusive(conn, csv_writer):

    # Compute total elapsed time in "tsc" units for the whole data-set.
    #
    # Other time values will be scaled relative to this, because we care
    # more about ratios than absolutes, especially when comparing captures
    # of differing total durations.
    #
    elapsed_tsc = conn.execute("""
        SELECT MAX(end_tsc) - MIN(start_tsc) FROM tmzones
    """).fetchone()[0]

    # An option to make the zone instances distinct at each depth level...
    #
    # (This may be useful to resolve confusion in the case of recursion,
    # or some other kind of broken nesting. However, in the general case,
    # we'll probably prefer to aggregate across different depths.)
    #
    do_separation_by_depth = False

    # We need to compute the total_tsc for each zone using a separate "pass"
    #
    # (Doing it inside the subsequent child-to-parent join would lead to
    #  over-counting on those parent zones which have multiple children!)
    #
    conn.executescript(
        """
        DROP VIEW IF EXISTS ZoneTotals;

        CREATE VIEW ZoneTotals AS

        SELECT
              SUM(end_tsc - start_tsc) AS total_tsc,
              fullname_id,
              depth
        FROM  tmzones
        GROUP BY fullname_id {depth_clause}
        """.format(
            depth_clause=", depth" if do_separation_by_depth else ""
        )
    )

    cur = conn.execute(
        """
        SELECT
              (zt.total_tsc -
                SUM(IFNULL(c.end_tsc - c.start_tsc, 0)))
                * 100000 / :elapsed_tsc    AS excl,

              zt.total_tsc
                * 100000 / :elapsed_tsc    AS incl,

              COUNT(DISTINCT p.start_tsc)  AS num_inst,

              t.content    AS name,

              p.thread_id  AS thread,

              COUNT(*)     AS num_parts,
              MIN(p.depth) AS min_depth,
              MAX(p.depth) AS max_depth

        FROM  tmzones AS p           -- 'p' for 'parent'

        -- A child zone must be a strict subzone of the parent
        -- and must be of the correct depth
        -- and from the same thread
        --
        -- NB: The "BETWEEN" clause forces efficient culling of the candidates
        --     via the indexing of start_tsc (as provided by zindex_delete)
        --
        LEFT JOIN
              tmzones AS c           -- 'c' for 'child' (on a self-join)
        ON    (c.start_tsc BETWEEN
              p.start_tsc AND p.end_tsc) AND    -- look for an overlapping...
              c.end_tsc <= p.end_tsc     AND    -- ...child zone
              c.depth = p.depth + 1      AND    -- ...at the correct depth
              c.thread_id = p.thread_id

        JOIN  tmexpandedtext as t       -- join against the text table to find our name
        ON    t.id = p.fullname_id

        JOIN  ZoneTotals as zt
        ON    zt.fullname_id = p.fullname_id
        {depth_clause_1}

        GROUP BY name {depth_clause_2}
        ORDER BY excl DESC

        """.format(
            depth_clause_1="AND zt.depth = p.depth" if do_separation_by_depth else "",
            depth_clause_2=", p.depth" if do_separation_by_depth else ""
        ),

        {"elapsed_tsc": elapsed_tsc}
    )

    write_from_cursor(cur, csv_writer)
