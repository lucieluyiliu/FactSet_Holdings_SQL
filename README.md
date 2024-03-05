# FactSet_Onwership_Security

This repository contains PostgreSQL code that aggregates 13F and fund-level reports in FactSet at the security level to quarterly holdings, dollar holdings, security-level and firm-level ownership. The data cleaning procedure follows the SAS code of Ferreria and Matos (2008, JFE):

1. Last availabel reports are rolled over to fill in missing report if it is less than 8 quarters old.
2. Fills in missing reports if the most recent report was filled after T-3
3. When both 13F reports and fund reports are available for a institution-security-quarter observation, use 13F for US securities, use the maximum holding of 13F and fund reports for non-US securities.