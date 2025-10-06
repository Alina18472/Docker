# -*- coding: utf-8 -*-
import sys

def main():
    # Словарь для хранения всех результатов
    results = {
        'vowel': 0,
        'consonant': 0,
        'word_count': 0,
        'total_chars': 0
    }
    
    # Читаем и суммируем все
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        
        try:
            key, value = line.split('\t')
            value = int(value)
            results[key] += value
        except:
            continue
    
    # Вычисляем среднюю длину слова
    if results['word_count'] > 0:
        avg_length = results['total_chars'] / results['word_count']
        print("average_word_length\t{:.2f}".format(avg_length))
    
    # Выводим 
    print("vowel\t{}".format(results['vowel']))
    print("consonant\t{}".format(results['consonant']))
    print("total_letters\t{}".format(results['vowel'] + results['consonant']))
    print("word_count\t{}".format(results['word_count']))


if __name__ == "__main__":
    main()