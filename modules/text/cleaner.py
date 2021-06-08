import re
import nltk
from striprtf.striprtf import rtf_to_text
from modules.text import tree_stem
from modules.database import models


def clean_rtf(rtf_bytes):
    texts = rtf_to_text(rtf_bytes).encode("cp1252", errors="ignore").decode("cp1251")
    # Select paragraphs that starts with "отже"
    texts = re.findall("\n(отже.*)\n", texts.lower())
    # Remove apostrophes
    texts = [re.sub("['`’]", "", t) for t in texts]
    # Replace all except cyrillic with spaces
    texts = [re.sub("[^АБВГҐДЕЄЖЗИІЇЙКЛМНОПРСТУФХЦЧШЩЬЮЯабвгґдеєжзиіїйклмнопрстуфхцчшщьюя]", " ", t) for t in texts]
    # Tokenize paragraphs
    texts = [nltk.word_tokenize(text, language='russian') for text in texts]
    # Remove stop and short (less then 3 characters) words
    stop_words = models.read_stop_words()
    texts = [[word for word in text if len(word) > 3 and word not in stop_words] for text in texts]
    # Stemitize words
    texts = [[tree_stem.stem_word(word) for word in text] for text in texts]
    return texts
