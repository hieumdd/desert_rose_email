from functools import reduce


def compose(*func):
    def _compose(f, g):
        return lambda x: f(g(x))

    return reduce(_compose, func, lambda x: x)
