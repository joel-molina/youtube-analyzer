import matplotlib.pyplot as plt
import pandas as pd
import html
import re
from matplotlib.ticker import MaxNLocator

plt.rcParams['axes.formatter.useoffset'] = False
plt.rcParams['axes.formatter.limits'] = (-5, 5)

def create_histogram(file_path):
    category_freq = {}

    #read file
    with open(file_path, 'r', encoding='utf-8') as file:
        for line in file:
            #split line by either tab or multiple spaces (weird .txt file)
            parts = re.split(r'\t+|\s{2,}', line.strip())

            #decode broken html '&' symbols
            category = html.unescape(parts[0])
            frequency = int(parts[1])

            #merge frequencies for decoded duplicate categories
            category_freq[category] = category_freq.get(category, 0) + frequency
            
    #swap to pandas series for quick sorting function.
    freq_series = pd.Series(category_freq).sort_values(ascending=False)

    # Plotting
    plt.figure(figsize=(12, 8))

    freq_series.plot(kind='barh', color='blue', edgecolor='black')#, log=True)
    plt.xlabel('Frequency')
    plt.ylabel('Category')
    plt.title('Category Frequency Histogram')

    #turn off scientific notation
    #place integer ticks on x-axis
    plt.gca().xaxis.set_major_locator(MaxNLocator(integer=True))
    #format to set commas to thousands and no decimal places in the value.
    plt.gca().xaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f'{x:,.0f}'))

    plt.tight_layout()
    plt.show()

#run
file_path = 'category_results.txt'
create_histogram(file_path)
