import matplotlib.pyplot as plt
import pandas as pd

def create_histograms(file_path):
    in_degrees = []
    out_degrees = []
    
    #read file
    with open(file_path, 'r') as file:
        for line in file:
            #check that line has statistics
            if 'In-Degree' in line and 'Out-Degree' in line:
                #remove whitespace and seperate videoID from stats
                parts = line.strip().split("\t") 

                #seperate in-degree and out-degree statistics
                in_degree_part = parts[1].split(",")[0]
                out_degree_part = parts[1].split(",")[1]

                #split at the colon and take the int value
                in_degree = int(in_degree_part.split(":")[1].strip())
                out_degree = int(out_degree_part.split(":")[1].strip())

                #append to array.
                in_degrees.append(in_degree)
                out_degrees.append(out_degree)
    
    #pandas series is WAY more efficient for rendering these graphs.
    in_series = pd.Series(in_degrees)
    out_series = pd.Series(out_degrees)
    combined_series = in_series + out_series

    #create subplots
    fig, axes = plt.subplots(1, 3, figsize=(18, 5))

    #in-degree histogram
    in_series.plot.hist(bins=20, color='blue', edgecolor='black', log=True, ax=axes[0])
    axes[0].set_title('In-Degree Histogram')
    axes[0].set_xlabel('In-Degree')
    axes[0].set_ylabel('Frequency')

    #out-degree histogram
    out_series.plot.hist(bins=20, color='purple', edgecolor='black', log=True, ax=axes[1])
    axes[1].set_title('Out-Degree Histogram')
    axes[1].set_xlabel('Out-Degree')
    axes[1].set_ylabel('Frequency')

    #combined-degree histogram
    combined_series.plot.hist(bins=20, color='green', edgecolor='black', log=True, ax=axes[2])
    axes[2].set_title('Combined Degree Histogram')
    axes[2].set_xlabel('In-Degree + Out-Degree')
    axes[2].set_ylabel('Frequency')

    plt.tight_layout()
    plt.show()

#run 
file_path = 'results.txt'  
create_histograms(file_path)
