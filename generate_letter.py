from docx import Document
import os

def generate_offer_letter(name, role, amount, start_date, location):
    doc = Document("offer_template.docx")
    for para in doc.paragraphs:
        para.text = para.text.replace("{{NAME}}", name)
        para.text = para.text.replace("{{ROLE}}", role)
        para.text = para.text.replace("{{AMOUNT}}", amount)
        para.text = para.text.replace("{{DATE}}", start_date)
        para.text = para.text.replace("{{LOCATION}}", location)

    output_dir = "offers"
    os.makedirs(output_dir, exist_ok=True)
    file_path = os.path.join(output_dir, f"{name}_offer_letter.docx")
    doc.save(file_path)
    return file_path