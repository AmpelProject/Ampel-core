from ampel.core.AmpelContext import AmpelContext

context: AmpelContext

reveal_type(context.config.get()) # noqa
reveal_type(context.config.get(None, None)) # noqa
reveal_type(context.config.get(None, dict)) # noqa
reveal_type(context.config.get(None, int)) # noqa
reveal_type(context.config.get(None, int)) # noqa
reveal_type(context.config.get(None, None, raise_exc=True)) # noqa
reveal_type(context.config.get(None, None, raise_exc=False)) # noqa
reveal_type(context.config.get(None, dict, raise_exc=True)) # noqa
reveal_type(context.config.get(None, int, raise_exc=False)) # noqa
reveal_type(context.config.get(None, raise_exc=True)) # noqa
reveal_type(context.config.get(None, raise_exc=False)) # noqa
reveal_type(context.config.get('db')) # noqa
reveal_type(context.config.get('db', None, raise_exc=False)) # noqa
reveal_type(context.config.get('db', raise_exc=True)) # noqa
reveal_type(context.config.get('db', list)) # noqa
reveal_type(context.config.get('db', list, raise_exc=True)) # noqa
