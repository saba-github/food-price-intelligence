@echo off
cd /d C:\Users\PC\Desktop\Projects\food-price-intelligence
call .venv\Scripts\activate.bat
python -m pipeline.run_pipeline --retailer a101 --category fruit_veg
pause