# -*- coding: utf-8 -*-

import logging
import os
import pwd
import tempfile

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='Plugin: Geocoder | %(levelname)s - %(message)s')


class CustomTmpFile(object):

    def __init__(self):
        self.cache_relative_dir = '.cache/dss/plugins/geocoder'
        self.tmp_output_dir = None

    def get_cache_location_from_user_config(self):
        """
        Return a per user cache location that is UIF safe
        :return: absolute location of cache
        """
        home_dir = pwd.getpwuid(os.getuid()).pw_dir
        cache_absolute_dir = os.path.join(home_dir, self.cache_relative_dir)
        if not os.path.exists(cache_absolute_dir):
            os.makedirs(cache_absolute_dir)
        return cache_absolute_dir

    def get_temporary_cache_dir(self):
        """
        Return a temporary directory with random name, output path will be:

            per_uid_cache_dir/random_dir/

        :return:
        """
        cache_absolute_path = self.get_cache_location_from_user_config()
        self.tmp_output_dir = tempfile.TemporaryDirectory(dir=cache_absolute_path)
        return self.tmp_output_dir

    def get_temporary_cache_file(self, output_file_name):
        """
        Return temporary file in cache with name `ouput_file_name`,
        the structure of the directory will be the following:

            per_uid_cache_dir/random_dir/output_file_name

        :param output_file_name:
        :return:
        """
        in_cache_random_dir = self.get_temporary_cache_dir()
        output_file = os.path.join(in_cache_random_dir.name, output_file_name)
        return output_file

    def clean(self):
        """
        Remove cache
        :return:
        """
        self.tmp_output_dir.cleanup()
