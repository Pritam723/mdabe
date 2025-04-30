cd mdabeVenv\Scripts
call activate.bat
cd..
cd..
cd app
flask --app app --debug run --host=0.0.0.0