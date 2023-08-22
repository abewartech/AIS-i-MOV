from itertools import islice


def read_file_lines(file_io, start=0, num_lines=10000):
    with open(file_io, "r") as file:
        chunk_of_lines = list(islice(file, start, start + num_lines))
        chunk_of_lines = [f for f in chunk_of_lines if f != "\\"]
        chunk_of_lines = [f for f in chunk_of_lines if f]
        return chunk_of_lines


def stream_file_per_chunk(file_io, start=0, num_lines=10000, end_file=None):
    while True:
        if end_file:
            if end_file < start:
                yield None
                break
        lines = read_file_lines(file_io, start, num_lines)
        if not lines:
            break
        start += len(lines)
        yield lines
