if not DEFINED IS_MINIMIZED set IS_MINIMIZED=1 && start "" /min "%~dpnx0" %* && exit
C:\Users\shahb\Documents\Glow\lead-prospect-data-discovery\env\Scripts\activate.bat && python "C:\Users\shahb\Documents\Glow\lead-prospect-data-discovery\AE Board\update_hubspot_base.py" && python "C:\Users\shahb\Documents\Glow\lead-prospect-data-discovery\AE Board\update_ae_board.py" && cd "C:\Users\shahb\Documents\Glow\lead-prospect-data-discovery\Glow Statement" && python "C:\Users\shahb\Documents\Glow\lead-prospect-data-discovery\Glow Statement\update_rollup.py" && deactivate && exit