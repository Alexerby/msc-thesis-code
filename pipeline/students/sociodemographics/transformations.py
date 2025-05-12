"""
sociodemographics/transformations.py

This module contains functions for transforming sociodemographic data within the data processing pipeline.
The transformations include data cleaning, feature engineering, and data enrichment tasks that prepare
the data for further analysis and calculations.

Functions in this module are designed to be modular and reusable, allowing for easy integration into
the broader data processing workflow. Each function focuses on a specific transformation task, ensuring
clarity and maintainability of the code.

Key functionalities include:
- Data cleaning: Handling missing values, correcting data types, and removing duplicates.
- Feature engineering: Creating new features or modifying existing ones to enhance the dataset.
- Data enrichment: Adding additional information to the dataset through merging or derived metrics.
- Data reshaping: Changing the structure of the data for analysis.

Usage:
Import the necessary functions from this module into your data processing script or pipeline to apply
the desired transformations to your sociodemographic data.

Example:
    from sociodemographics.transformations import clean_data, encode_categorical

    df_cleaned = clean_data(df)
    df_encoded = encode_categorical(df_cleaned)
"""

