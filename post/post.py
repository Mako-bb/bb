# -*- coding: utf-8 -*-
import importlib

class PostScraping():
    def __init__(self, platform_code, created_at):
        self._platform_code = platform_code
        self._created_at = created_at

        module_name = platform_code.split('.')[-1].replace('-', '')
        self._module = importlib.import_module('post.' + module_name)

    def run(self):
        post_images = self._module.PostScraping(self._platform_code, self._created_at)
        post_images.run()
