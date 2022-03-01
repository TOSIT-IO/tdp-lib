import argparse
import os


class EnvDefault(argparse.Action):
    # Inspired greatly from https://stackoverflow.com/a/10551190
    def __init__(self, envvar, required=True, default=None, help=None, **kwargs):
        if not default and envvar:
            if envvar in os.environ:
                default = os.environ[envvar]
            if help:
                help += f", settable through `{envvar}` environment variable"
        if required and default:
            required = False
        super(EnvDefault, self).__init__(
            default=default, required=required, help=help, **kwargs
        )

    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, values)
