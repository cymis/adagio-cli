from parsl import python_app, join_app

from qiime2.sdk.proxy import ProxyResults, Proxy
from qiime2.sdk import Pipeline, Results
from qiime2.sdk.context import ParallelContext

from adagio.execution.proxy import IndexedProxyResults, dfk_thread_future, lift_parsl


class AdagioContext(ParallelContext):
    def __init__(self, action_obj=None, parent=None):
        super().__init__(action_obj, parent)


    def _callable_action_(self, *args, **kwargs):
        # The function is the first arg, we ditch that
        args = args[1:]

        # If we have a named_pool, we need to check for cached results that
        # we can reuse.
        #
        # We can short circuit our index checking if any of our arguments
        # are proxies because if we got a proxy as an argument, we know it
        # is a new thing we are computing from a prior step in the pipeline
        # and thus will not be cached.
        if self.cache.named_pool is not None and \
                not _contains_proxies(*args, **kwargs) and \
                (cached_results := self._check_cache(args, kwargs)):
            return cached_results

        # If we didn't have cached results to reuse, we need to execute
        # the action.
        return self._dispatch_(*args, **kwargs)

    def _dispatch_(self, *args, **kwargs):
        action = self.action_obj
        print("SCHEDULED:", action.plugin_id, action.id)

        inputs = action.signature.collate_inputs(*args, **kwargs)
        output_types = action.signature.solve_output(**inputs)
        bound_action = action._bind(lambda: self, {"type": "adagio-cli"})
        def output_wrapper(future):
            return IndexedProxyResults(future, output_types)

        if isinstance(action, Pipeline):
            decorator = lift_parsl(output_wrapper, join=True)
        else:
            decorator = lift_parsl(output_wrapper)

        proxy = decorator(bound_action)(*args, **kwargs)
        proxy._future_.add_done_callback(lambda x: print('DONE:', action.plugin_id, action.id))
        return proxy

    def _make_alias_(self, result, name, provenance):
        if isinstance(result, Proxy):
            own_future = result._future_[result._selector_]
            alias_future = dfk_thread_future(_deferred_alias)(
                own_future, name, provenance, self)
            return result.__class__([alias_future], 0)
        else:
            # only our proxies call this at the moment.
            # if you are here, then result must be a real artifact
            raise NotImplementedError('impossible')


def _contains_proxies(*args, **kwargs):
    """Returns True if any of the args or kwargs are proxies
    """
    return any(isinstance(arg, Proxy) for arg in args) \
        or any(isinstance(value, Proxy) for
               value in kwargs.values())


def _deferred_alias(artifact, name, provenance, ctx):
    return artifact._alias(name, provenance, ctx)
