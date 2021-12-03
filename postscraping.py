# -*- coding: utf-8 -*-
from post import post

if __name__ == "__main__":
    platform_code = 'us.amazon'
    created_at = '2019-11-15'
    post_images = post.PostScraping(platform_code, created_at)
    post_images.run()
