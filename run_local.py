from dotenv import load_dotenv
load_dotenv()

import main
main.flask_app.run(host="0.0.0.0", port=5000, debug=False)
