"""
    OCR Utility Classes
"""
from PIL import Image, ImageSequence
from io import BytesIO
import pytesseract
import re
import logging


class OCR:
    def convert_image_to_text(self, filename=None, image=None):
        logging.info('convert image to text')
        """
        :param filename: Path to the image file name
        :param image: PIL Image object. Either filename or PIL Image should be passed to this function
        :return: array, containing individual page content as string
        """
        if filename is None and image is None:
            raise Exception("Both file name and image cannot be none...")

        im = None

        if filename:
            im = Image.open(filename)
        else:
            im = image

        logging.info("OCR for file %s started" % (filename))
        full_content = []
        for i, page in enumerate(ImageSequence.Iterator(im)):
            temp = BytesIO()
            page.save(temp, format="png")
            single_page_image = Image.open(temp)
            logging.info("Detecting angle for page %d started" % (i))
            angle = self.__detect_angle__(single_page_image)
            logging.info("Detecting angle for page %d ended" % (i))
            if angle != 0:
                logging.info("Rotating page %d started" % (i))
                rotated_image = self.__rotate__(single_page_image, current_angle=angle)
                logging.info("Rotating page %d ended" % (i))
            else:
                rotated_image = single_page_image
                logging.info("Single Image")
            text = pytesseract.image_to_string(rotated_image,lang='eng')
            logging.info(pytesseract.get_tesseract_version)
            full_content.append(text)
        logging.info("OCR for file %s ended" % (filename))
        return full_content

    def __detect_angle__(self, image):
        """
        Detect angle of an image.
        :param image: PIL image object
        :return: current angle of the image (90/180/270)
        """
        try:
            meta_data = pytesseract.image_to_osd(image)
            angle = re.search('(?<=Rotate: )\d+', meta_data).group(0)
            return int(angle)
        except (Exception) as error:
            logging.info('***************Exception in detect angle ******************')
            return int(90)

    def __rotate__(self, image, current_angle=0):
        """
        :param image: PIL Image Object
        :param current_angle: Current angle of the document
        :return: rotated image object
        """
        angle = 360 - current_angle
        rotate_angle = Image.ROTATE_90
        if angle == 90:
            rotate_angle = Image.ROTATE_90
        if angle == 180:
            rotate_angle = Image.ROTATE_180
        if angle == 270:
            rotate_angle = Image.ROTATE_270
        return image.transpose(rotate_angle)