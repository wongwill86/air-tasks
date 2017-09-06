from celery import loaders
import celeryconfig


BaseLoader = loaders.get_loader_cls('app')


class CeleryLoader(BaseLoader):
    def config_from_object(self, obj, silent=False):
        print('calling custom config')
        super(CeleryLoader, self).config_from_object(celeryconfig, silent)
