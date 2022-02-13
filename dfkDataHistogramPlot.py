import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
import matplotlib.ticker as ticker
# %matplotlib inline


# set dark grid style for our plot
sns.set(style='darkgrid')

dataCSV = './nftTopUsers.csv'

def plotBarGraph():
    df = pd.read_csv(dataCSV, header=0)
    mainClassesDF = df["mainClass"]
    ncount = len(mainClassesDF)
    print("Length of mainClasses are:", ncount)

    plt.figure(figsize=(30,16))
    ax = sns.countplot(mainClassesDF, order=mainClassesDF.value_counts().index)
    plt.title('Distribution of Heroes\' main class in DeFi Kingdoms for sample 90000 records')

    for p in ax.patches:
        counts = p.get_height()
        perDistribution = (counts * 100 / ncount)
        pVal = "{:.2f}".format(perDistribution)
        annotatedText = "{counts}, {percentage}%".format(counts=p.get_height(), percentage=pVal)
        ax.annotate(annotatedText, (p.get_x() + p.get_width() / 2., p.get_height()), ha = 'center', va = 'center', xytext = (0, 15), textcoords = 'offset points')
    
    plt.show()
    plt.savefig('heroesdistribution.png')


def main():
    print("Plotting the heroes distribution in DeFi Kingdom")
    plotBarGraph()
    print("Program Ended")
    
# boiler plate code for main execution
if __name__ == "__main__":
    main()