from qiime2.sdk.proxy import (Proxy, ProxyResult, ProxyArtifact, ProxyResults,
                              ProxyResultCollection)

from parsl import python_app



class ProxyMetadata(Proxy):
    def __init__(self, future, selector=NotImplemented):
        self._future_ = future
        self._selector_ = selector

    def get_column(self, name):
        return ProxyMetadataColumn(self._future_, name)

    def _get_element_(self, metadata):
        return metadata

    def result(self):
        return self._future_.result()


class ProxyMetadataColumn(Proxy):
    def __init__(self, future, column):
        self._future_ = future
        self._selector_ = column

    def _get_element_(self, metadata):
        try:
            return metadata.get_column(self._selector_)
        except Exception:
            raise ValueError(dict(metadata=metadata, future = self._future_, sel=self._selector_))

    def result(self):
        return self._get_element_(self._future_.result())


# I'm sorry. This call stack will unwind someday and somewhere.
# use as `@lift_parsl(MetadataProxy)`
def lift_parsl(output_wrapper):
    # actual decorator, you will not typically see this as python evals it
    def decorator(actual_callable):
        # `actual_callable` is replaced by `lifted_callable`
        # and can be called without knowledge of parsl, it will compose
        # with other lifted operations via Proxy objects
        def lifted_callable(**external_kwargs):
            # parsl treats `inputs` as a variadic set of futures
            # so inspect the `external_kwargs` for our proxies and
            # collect the futures into a spot parsl will see them
            internal_kwargs = kwargs_to_parsl(**external_kwargs)
            def _parsl_app(**internal_kwargs):
                # convert `inputs` and proxies into realized objects
                # (if we are here, then the source appfutures have resolved)
                realized_kwargs = kwargs_from_parsl(**internal_kwargs)

                results = actual_callable(**realized_kwargs)
                # these will be wrapped in an appfuture by parsl
                return results

            # The main operations of a Monad, if you were interested...

            # the prophesized `lift`
            submittable = python_app(_parsl_app)
            # the implied `bind` which composes lifted operations together
            future = submittable(**internal_kwargs)
            # the return of a `unit` legible to bind/compose
            return output_wrapper(future)

        lifted_callable.__wrapped__ = actual_callable
        return lifted_callable
    return decorator


def kwargs_to_parsl(**kwargs):
    inputs = []
    selectors = []
    raw = {}

    for key, value in kwargs.items():
        # even collections will be a single Proxy (of many inner dependencies)
        if isinstance(value, Proxy):
            future, selector = _detach(value)
            inputs.append(future)
            selectors.append((key, selector))
        else:
            raw[key] = value

    return dict(inputs=inputs, selectors=selectors, raw=raw)


def kwargs_from_parsl(inputs, selectors, raw):
    kwargs = {**raw}
    for future, (key, selector) in zip(inputs, selectors):
        kwargs[key] = selector(future)
    return kwargs


def _detach(value):
    if isinstance(value, (ProxyResults, ProxyMetadata)):
        future = value._future_
        return (future, lambda result: result)
    elif isinstance(value, (ProxyResult, ProxyResultCollection)):
        future = value._future_
        # access outside of closure for dill
        sel = value._selector_
        return (future, lambda result: getattr(result, sel))
    elif isinstance(value, ProxyMetadataColumn):
        future = value._future_
        # access outside of closure for dill
        sel = value._selector_
        return (future, lambda result: result.get_column(sel))
    else:
        raise NotImplementedError