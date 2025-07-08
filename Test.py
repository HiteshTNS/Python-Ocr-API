# from PIL import Image
# import pytesseract
#
# pytesseract.pytesseract.tesseract_cmd = r'C:\Users\hitesh.paliwal\AppData\Local\Programs\Tesseract-OCR\tesseract.exe'
# img = Image.open('test_image.png')
# print(pytesseract.image_to_string(img))


def ocr_pdf_page(pdf_path):
    import pytesseract
    from pdf2image import convert_from_path
    from PIL import Image

    pytesseract.pytesseract.tesseract_cmd = r'C:\Users\hitesh.paliwal\AppData\Local\Programs\Tesseract-OCR\tesseract.exe'
    poppler_path = r'C:\Users\hitesh.paliwal\Documents\poppler\Library\bin'  # Adjust as needed

    images = convert_from_path(pdf_path, dpi=300, poppler_path=poppler_path)
    for img in images:
        text = pytesseract.image_to_string(img, config="--psm 6")
        print(text)

if __name__ == "__main__":
    pdf_path=r"C:\Users\hitesh.paliwal\Downloads\VCI - claims PDF\71697875.pdf"
    ocr_pdf_page(pdf_path)
