from datamanager import Datamanager
from src.globals import domain


class Api:
    @staticmethod
    def request_api_catalog(amazon, token, index, cant_items):
        api = "{}/gp/video/api/search?queryToken={}&queryPageType=browse&ref_=" \
              "atv_sr_infinite_scroll&pageId=default&startIndex={}&useNodePlayer=1&totalItems={}" \
              "&refMarker=atv_sr_infinite_scroll&isRetail=1&isHover2019=1&ie=UTF8".format(domain(),token, index, cant_items)
        contenidos = Datamanager._getJSON(amazon, api, headers=amazon.headers)
        return contenidos['items']

    @staticmethod
    def api_collection(token):
        return "{}/gp/video/api/storefront/ref=atv_hm_nxt_2?startIndex=0&targetId=V2" \
               "%3D4AEA6u69gYPeuYe-toZwYWdlSWSIcGFnZVR5cGWMY29sbGVjdGlvbklkiHdpZGdldElkjo5zd2lmdElkVmVyc" \
               "2lvbt6UioRob21li4Rob21ljA-ND46CVjI%3D&pageSize=150&contentId=home&contentType=home&" \
               "serviceToken={}%3D&pageNumber=2".format(domain(),token)

    @staticmethod
    def api_channels(token):
        return "{}/gp/video/api/storefront/ref=atv_hm_nxt_2?contentId=default&" \
               "startIndex=0&pageSize=14&contentType=subscription&serviceToken={}&pageNumber=2&" \
               "targetId=V2%3D4AEA6u69gYPeuYe-toZwYWdlSWSIcGFnZVR5cGWMY29sbGVjdGlvbklkiHdpZGdld" \
               "Elkjo5zd2lmdElkVmVyc2lvbt6fiodkZWZhdWx0i4xzdWJzY3JpcHRpb26MD40PjoJWMg%3D%3D".format(domain(),token)

    @staticmethod
    def api_channel(channel, token):
        return "{}/gp/video/api/storefront/ref=atv_hm_nxt_2?serviceToken=" \
               "{}&startIndex=0&pageSize=50&contentType=subscription&pageNumber=2&contentId={}" \
               "&targetId=V2%3D4AEA6u69gYPeuYe-toZwYWdlSWSIcGFnZVR5cGWMY29sbGVjdGlvbklkiHdpZGd" \
               "ldElkjo5zd2lmdElkVmVyc2lvbt6bioNoYm-LjHN1YnNjcmlwdGlvbowPjQ-OglYy".format(domain(),token, channel)

    # India style
    @staticmethod
    def api_collection_pv(token):
        return "{}/api/storefront/ref=atv_hm_nxt_2?targetId=V2" \
               "%3D4AEA6u69gYPeuYe-toZwYWdlSWSIcGFnZVR5cGWMY29sbGVjdGlvbklkiHdpZGdldElkjo5zd2lmdElkVmVyc" \
               "2lvbt6UioRob21li4Rob21ljA-ND46CVjI%3D&startIndex=0&pageSize=150&contentId=home&contentType=home&" \
               "serviceToken={}%3D&pageNumber=2".format(domain(), token)
