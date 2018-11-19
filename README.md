![Python 3.5](https://img.shields.io/badge/Python-3.5-blue.svg)
![Pytorch](https://img.shields.io/badge/Pytroch-ready-brightgreen.svg)


This repository holds the code for my master thesis at the University of Konstanz, 2018.

# Towards-Crime-Forecasting-Using-Deep-Learning

Crime forecasting is one of the most wanted possible forecasts, as it could lead to fewer crimes and fewer police forces to secure threatened areas.
However, predicting when and where crime will happen is challenging.
Even modern predictive policing methods don't provide a reasonable
 or accurate approximation to forecast crimes.
It has a long history of experiments and tests to reduce and to prevent crime.
Deep Learning is a relatively new machine learning topic, which achieved state-of-the-art performance in many tasks and slowly but surely changes the machine learning area.
For a few tasks, it is even better than the human himself.

This success leads to the questions *"Is deep learning able to forecast crime as accurate or even better as a human?"* and *"How to forecast crime using deep learning techniques?"*.

This work presents two deep learning architectures to forecast crime for the cities, Chicago, San Francisco and Los Angeles, which start to answer the second question.
It presents a workflow to visual debug network architectures and to steer the architecture design for the learning process in a promising direction.
Further, it compares a similar deep learning forecast architecture to the two proposed ones and shows how good or bad the two designs can handle unseen data.
In the end, it concludes concerning the results and illustrates what is possible to enhance the methods.

#  Install and test

Installation can be done by installing the dependencies from the requirements.txt.
Afterwards the crawler script needs to be executed to grab all the required data.
Then the heatmaps for the respective cities can be created out of the crawled data.

Step-by-step:  
1. First crawl the data:  
```
python getCrimeData/crawler.py
```
2. Next convert raw crime data to heatmaps:
```
python generateHeatmaps/create_heatmaps_chicago.py
```
```
python generateHeatmaps/create_heatmaps_sanfran.py
```
