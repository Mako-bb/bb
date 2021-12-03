import time


class Login:
    @classmethod
    def login(cls, driver, pv_email, pv_password):
        driver.get("https://www.amazon.com/ap/signin?accountStatusPolicy=P1&clientContext=259-8268300-2599442&language=en_US&openid.assoc_handle=amzn_prime_video_desktop_us&openid.claimed_id=http%3A%2F%2Fspecs.openid.net%2Fauth%2F2.0%2Fidentifier_select&openid.identity=http%3A%2F%2Fspecs.openid.net%2Fauth%2F2.0%2Fidentifier_select&openid.mode=checkid_setup&openid.ns=http%3A%2F%2Fspecs.openid.net%2Fauth%2F2.0&openid.ns.pape=http%3A%2F%2Fspecs.openid.net%2Fextensions%2Fpape%2F1.0&openid.pape.max_auth_age=0&openid.return_to=https%3A%2F%2Fwww.primevideo.com%2Fregion%2Feu%2Fauth%2Freturn%2Fref%3Dav_auth_ap%3F_encoding%3DUTF8%26location%3D%252Fregion%252Feu%252Fref%253Ddv_auth_ret")
        email = driver.find_element_by_id('ap_email')
        password = driver.find_element_by_id('ap_password')
        sign_in_button = driver.find_element_by_id("signInSubmit")
        email.send_keys(pv_email)
        password.send_keys(pv_password)
        time.sleep(2)
        sign_in_button.click()
        time.sleep(10)
