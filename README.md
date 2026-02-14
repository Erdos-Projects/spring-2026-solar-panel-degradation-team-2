# spring-2026-solar-panel-degradation-team-2
Project completed for Erdos institute data science bootcamp (fall-2025)

### Team members
1. [Roberta Shapiro](https://github.com/ShapiroRH)
2. [Charles Baker](https://github.com/ch83baker)
3. [Alex Myers](https://github.com/MyersAlex924)
4. [William Grodzicki](https://github.com/wpgrodzicki)

## Project overview
We hope to develop tools for analyzing and predicting photovoltaic-cell degradation in solar panels using location, meteorological data, and installation type

## Motivation and problem statement
Solar power is a growing portion of energy production in the United States and will likely continue to play a significant role for the foreseeable future. As such, it is important to understand and be able to predict the long term state of solar farms, in order to plan for future growth, replacement, and maintenance.

In this project we seek to answer the following question: “Can we use meteorological data and patterns to predict future degradation of solar cells?”

## Stakeholders
Our primary stakeholders are companies and governments that are planning to construct and/or maintain solar farms, as this project aims to predict their long term health and performance.

## Dataset
We obtained our primary datasets from the Open Energy Data Initiative (OEDI) Photovoltaic Data Acquisition (PVDAQ) Public Datasets. Our target variable is cell efficacy as measured by irradiance.

Our predictive features include latitude, longitude, ZIP, meteorological data, and type of installation


## Modeling approach
Our predictive features are currently all categorical, so we are using one hot encoding with dummy variables. This is expected to change.
