def chunked(iterable, size):
    """
    Splits an iterable into chunks of a specified size.

    Args:
        iterable: An iterable to be split into chunks.
        size (int): The size of each chunk.

    Yields:
        list: Lists containing up to `size` elements from the iterable. The last chunk may contain fewer elements if the total number is not divisible by `size`.
    """
    buf = []
    for x in iterable:
        buf.append(x)
        if len(buf) == size:
            yield buf
            buf = []
    if buf:
        yield buf
