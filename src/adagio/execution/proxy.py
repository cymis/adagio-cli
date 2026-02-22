from qiime2.sdk.proxy import (
    Proxy, ProxyResult, ProxyVisualization, ProxyArtifact, ProxyResults,
    ProxyResultCollection)
from qiime2.core.type.util import is_visualization_type, is_collection_type
from parsl import python_app, join_app



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
        return metadata.get_column(self._selector_)

    def result(self):
        return self._get_element_(self._future_.result())


class IndexedProxyResults(ProxyResults):
    def _create_proxy(self, selector):
        qiime_type = self._signature_[selector].qiime_type
        idx = list(self._signature_.keys()).index(selector)

        if is_collection_type(qiime_type):
            return IndexedProxyResultCollection(
                self._future_, idx, qiime_type)
        elif is_visualization_type(qiime_type):
            return IndexedProxyVisualization(
                self._future_, idx, qiime_type)

        return IndexedProxyArtifact(self._future_, idx, qiime_type)


class IndexedProxyArtifact(ProxyArtifact):
    def _get_element_(self, results):
        return results[self._selector_]

    def _alias(self, name, provenance, ctx):
        return ctx._make_alias_(self, name, provenance)


class IndexedProxyVisualization(ProxyVisualization):
    def _get_element_(self, results):
        return results[self._selector_]

    def _alias(self, name, provenance, ctx):
        return ctx._make_alias_(self, name, provenance)


class IndexedProxyResultCollection(ProxyResultCollection):
    def _get_element_(self, results):
        return results[self._selector_]


# I'm sorry. This call stack will unwind someday and somewhere.
# use as `@lift_parsl(MetadataProxy)`
def lift_parsl(output_wrapper, join=False):
    # actual decorator, you will not typically see this as python evals it
    def decorator(actual_callable):
        # `actual_callable` is replaced by `lifted_callable`
        # and can be called without knowledge of parsl, it will compose
        # with other lifted operations via Proxy objects
        def lifted_callable(*ext_args, **ext_kwargs):
            # parsl treats `inputs` as a variadic set of futures
            # so inspect the `external_kwargs` for our proxies and
            # collect the futures into a spot parsl will see them
            int_kwargs = kwargs_to_parsl(*ext_args, **ext_kwargs)
            def _parsl_app(**int_kwargs):
                # convert `inputs` and proxies into realized objects
                # (if we are here, then the source appfutures have resolved)
                real_args, real_kwargs = kwargs_from_parsl(**int_kwargs)
                results = actual_callable(*real_args, **real_kwargs)
                if join:
                    return _to_futures(results)
                else:
                    # these will be wrapped in an appfuture by parsl
                    return results

            # The main operations of a Monad, if you were interested...

            # the prophesized `lift`
            if join:
                submittable = join_app(_parsl_app)
            else:
                submittable = python_app(_parsl_app)
            # the implied `bind` which composes lifted operations together
            future = submittable(**int_kwargs)
            # the return of a `unit` legible to bind/compose
            return output_wrapper(future)

        lifted_callable.__wrapped__ = actual_callable
        return lifted_callable
    return decorator


def _to_futures(results):
    futures = []
    for result in results:
        if isinstance(result, Proxy):
            # IndexedProxies can dereference themselves from
            # either a list of futures from parsl, or a future
            # results object, they will have the same positional index.
            future = result._future_[result._selector_]
        else:
            future = future_value(result)
        futures.append(future)
    return futures


def future_value(value):
    return dfk_thread_future(lambda x: x)(value)


def dfk_thread_future(callable):
    def wrapped(*args, **kwargs):
        app = python_app(callable, executors=['_parsl_internal'])
        return app(*args, **kwargs)
    return wrapped



def kwargs_to_parsl(*args, **kwargs):
    new = []
    inputs = []
    selectors = []
    raw = {}

    for value in args:
        if isinstance(value, Proxy):
            future, selector = _detach(value)
            inputs.append(future)
            selectors.append((None, selector))
        else:
            new.append(value)
            # keep track of order
            inputs.append(None)
            selectors.append((None, None))

    for key, value in kwargs.items():
        # even collections will be a single Proxy (of many inner dependencies)
        if isinstance(value, Proxy):
            future, selector = _detach(value)
            inputs.append(future)
            selectors.append((key, selector))
        else:
            raw[key] = value

    return dict(args=new, inputs=inputs, selectors=selectors, raw=raw)


def kwargs_from_parsl(args, inputs, selectors, raw):
    kwargs = {**raw}
    new = []
    for future, (key, selector) in zip(inputs, selectors):
        if future is None:
            new.append(args.pop(0))
        elif key is None:
            try:
                new.append(selector(future))
            except:
                raise Exception(selector(range(10)))
        else:
            kwargs[key] = selector(future)
    return [*new, *args], kwargs


def _detach(value):
    if isinstance(value, (ProxyResults, ProxyMetadata)):
        future = value._future_
        return (future, lambda result: result)
    elif isinstance(value, (ProxyResult, ProxyResultCollection)):
        future = value._future_
        # access outside of closure for dill
        sel = value._selector_
        return (future, lambda result: result[sel])
    elif isinstance(value, ProxyMetadataColumn):
        future = value._future_
        # access outside of closure for dill
        sel = value._selector_
        return (future, lambda result: result.get_column(sel))
    else:
        raise NotImplementedError