### Directory Purpose
To house the Python and Jupyter notebooks for quant/systematic investing and trading products
1) Conversion routine from Quicken portfolio ledger to Tradesviz import file
2) Routines to setup and maintain fundamental & pricing database
3) Relative strength computation
4) Implied volatilty on option capable securities
5) Stock filtering and alerts 
6) Relative Rotation Graphing
7) IB Order framework
8) Monte Carlo simulator
9) Strategy backtesting
10) Streamlit frontend
11) AI/ML Implementations where useful

### System Goals and Directions:
1)	Tiingo initial load pipeline for prices
2)	Tiingo maintenance load pipeline for prices
3)	FMP initial load pipeline for fundamentals
4)	FMP maintenance load for fundamentals
5)	Take existing relative strength study and adapt to local database of prices with fundamental filtering
6)	Take existing relative rotation graph study and adapt to local database
7)	Strategy exploration
8)	Strategy backtesting
9)	Strategy implementation in Interactive Brokers for paper trading
10)	Loop steps 7-9 to add and vet strategies
11)	Deployment of winning strategies in real portfolio with money
12)	Build in additional asset types
13)	Incorporate AI/ML models where appropriate

```
 -- Directory Structure --
quant_lab/
├── database                     # Directory of database creation, ETL routines, and stored procs/functions
├── data_pipelines               # Location of calling pipelines for API downloads to database
├── notebooks                    # Jupyter notebooks for prototyping
├── secrets                      # Directory of API keys and database URLs  - not in GitHub
├── strategies                   # Location of strategies and backtesting - not in GitHub
├── streamlit_app                # Root directory for Streamlit UI with subsequent pages
├── util                         # Location of central classes used in most Python
└── workdir                      # Temporary and utility files & directories to eventually be used - not in GitHub 

```
