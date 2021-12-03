import random
import urllib
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
import os
import sys
import time
import requests

class Captcha_bypass():
    """Funcionamiento del Script explicado en este video:
    https://www.youtube.com/watch?v=fsF7enQY8uI&ab_channel=MattUnsworthMattUnsworth
    """
    def __init__(self,driver):
        pass
    def _click_captcha(driver):
        iframes = driver.find_elements_by_tag_name('iframe')
        audio_button_found = False,
        audio_button_index = -1
        for index in range(len(iframes)):
            driver.switch_to.default_content()
            iframe = driver.find_elements_by_tag_name('iframe')[index]
            driver.switch_to.frame(iframe)
            driver.implicitly_wait(5)
        driver.find_element_by_id('recaptcha-anchor').click()
    # def __init__(self,driver):
    #     self.speech_to_text_url = "https://azure.microsoft.com/es-es/services/cognitive-services/speech-to-text/#features"
    #     audio_file = "\\payload.mp3"
    #     self.iframe = 0
    #     self.index = 0
    #     self.delay()
    #     time.sleep(2)
    #     # driver.execute_script('''window.open("","_blank")''')
    #     # driver.switch_to.window(driver.window_handles[1])
    #     # driver.get('https://www.online-convert.com/es/entrar')
    #     # driver.find_element_by_id('email_login').send_keys('brunodiaz1154@gmail.com')
    #     # driver.find_element_by_id('password_login').send_keys('darwin1154')
    #     # time.sleep(0.5)
    #     # driver.find_element_by_id('btn-extension-cancel').click()
    #     time.sleep(0.5)
    #     #driver.find_element_by_id('submit_button').click()
    #     #driver.close()
    #     #driver.switch_to.window(driver.window_handles[0])
    #     counter = 0
    #     while True:
    #         x = self._captcha_complete(driver,audio_file,counter)
    #         counter+=1
    #         if x == None:
    #             break
        
    def _captcha_complete(self,driver,audio_file,counter=0):
        try:
            audio_button_found, audio_button_index = self.iframe_finder(driver,counter)
        except:
            return None
        if audio_button_found:
            try:
                src = driver.find_element_by_id('audio-source').get_attribute('src')
            except:
                try:
                    src = driver.find_element_by_class_name('rc-audiochallenge-tdownload-link').get_attribute('href')
                except:
                    return None

            tex_ = urllib.request.urlretrieve(src,os.getcwd()+"\\analysis"+ audio_file)
            self.wav_converter(os.getcwd()+"\\analysis"+audio_file,driver)
            key = self.audio_to_text(os.getcwd()+"\\analysis\\payload.wav",driver)
            driver.switch_to.default_content()
            try:
                iframe = driver.find_elements_by_tag_name('iframe')[audio_button_index]
                driver.switch_to.frame(iframe)
            except IndexError:
                a = "A"
            
            input_field = driver.find_element_by_id('audio-response').send_keys(key)
            time.sleep(2)
            driver.find_element_by_id('recaptcha-verify-button').click()
            return "a"
    def wav_converter(self,audiofile,driver):
        driver.execute_script('''window.open("","_blank")''')
        driver.switch_to.window(driver.window_handles[1])
        driver.get('https://audio.online-convert.com/es/convertir-a-wav')
        driver.find_element_by_id('fileUploadInput').send_keys(audiofile)
        driver.find_element_by_id('multifile-submit-button-main').click()
        time.sleep(10)
        src = driver.find_element_by_class_name('file-link').get_attribute('href')
        urllib.request.urlretrieve(src,os.getcwd()+"\\analysis\\payload.wav")
        driver.close()
        driver.switch_to_window(driver.window_handles[0])

    def iframe_finder(self,driver,counter):
        if counter > 0:
            return self.iframe,self.index
        iframes = driver.find_elements_by_tag_name('iframe')
        audio_button_found = False,
        audio_button_index = -1
        for index in range(len(iframes)):
            driver.switch_to.default_content()
            iframe = driver.find_elements_by_tag_name('iframe')[index]
            driver.switch_to.frame(iframe)
            driver.implicitly_wait(5)
            try:
                audio_button= driver.find_element_by_id("recaptcha-audio-button")
                audio_button.click()
                audio_button_found = True
                audio_button_index = index
                self.index = audio_button_index
                self.iframe = audio_button_found
                return audio_button_found,audio_button_index
            except Exception as e:
                pass
    def delay(self):
        time.sleep(random.randint(2,5))
    def audio_to_text(self,audiofile,driver):
        driver.execute_script('''window.open("","_blank")''')
        base = os.path.splitext(audiofile)[0]
        driver.switch_to.window(driver.window_handles[1])
        driver.get(self.speech_to_text_url)
        self.delay()
        audio_input = driver.find_element(By.XPATH,'//*[@id="fileinput"]')
        audio_input.send_keys(audiofile)
        self.delay()
        time.sleep(10)
        text = driver.find_element(By.XPATH,'//*[@id="speechout"]')
        while text is None:
            text = driver.find_element(By.XPATH,'//*[@id="speechout"]')
        result = text.text
        clean_text = result.split('\n')[-1].replace('\n','').replace('-','')
        driver.close()
        driver.switch_to_window(driver.window_handles[0])
        os.remove(audiofile)
        return clean_text