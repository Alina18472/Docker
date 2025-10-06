# -*- coding: utf-8 -*-
import sys
import re

def is_vowel(char):
    vowels = "aeiouyаеёиоуыэюяAEIOUYАЕЁИОУЫЭЮЯ"
    return char in vowels

def is_consonant(char):
    consonants = "bcdfghjklmnpqrstvwxzбвгджзйклмнпрстфхцчшщBCDFGHJKLMNPQRSTVWXZБВГДЖЗЙКЛМНПРСТФХЦЧШЩ"
    return char in consonants

def main():
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        
        # Считаем гласные и согласные
        vowels = 0
        consonants = 0
        
        for char in line:
            if is_vowel(char):
                vowels += 1
            elif is_consonant(char):
                consonants += 1
        
        # Считаем слова
        words = re.findall(r'\b[а-яА-Яa-zA-Z]+\b', line)
        word_count = len(words)
        total_chars = sum(len(word) for word in words)
        
        # Выводим все что посчитали
        if vowels > 0:
            print("vowel\t{}".format(vowels))
        if consonants > 0:
            print("consonant\t{}".format(consonants))
        if word_count > 0:
            print("word_count\t{}".format(word_count))
            print("total_chars\t{}".format(total_chars))

if __name__ == "__main__":
    main()