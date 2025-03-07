# PostgreSQL TPC-H LAB

This repository provides PostgreSQL scripts for generating TPC-H benchmark data, creating database tables, running join experiments, and visualizing data histograms. It's useful for users working with TPC-H data in PostgreSQL for academic, research, or performance analysis.

## Table of Contents
1. [TPC-H Data Generation and Table Creation](#1-tpc-h-data-generation-and-table-creation)

2. [Join Experiment](#2-join-experiment)

3. [Data Histogram Plot](#3-data-histogram-plot)

## 1. TPC-H Data Generation and Table Creation
This module includes scripts to generate TPC-H benchmark data and create the appropriate PostgreSQL tables.
The relavant files are located at `./TPC-H_DataGen`.

## 2. Join Experiment
Scripts to run various SQL joins across TPC-H tables to measure query performance and retrieve meaningful insights from the data.
The relavant files are located at `./Join_Experiment`.

## 3. Data Histogram Plot
This module provides scripts to create histograms for data distribution analysis of specific TPC-H tables.
The relavant files are located at `./Data_Histogram`.
