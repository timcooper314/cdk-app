import base64
import http.server
import secrets
import urllib.parse
import webbrowser
import requests


# ----------------- CONFIGURATION -----------------
CLIENT_ID = ""
CLIENT_SECRET = ""
REDIRECT_URI = "http://127.0.0.1:8080"
# Add or remove scopes depending on your application needs
SCOPES = "user-top-read playlist-read-private playlist-modify-public playlist-modify-private"
# -------------------------------------------------

AUTHORIZATION_CODE = None
STATE_KEY = secrets.token_urlsafe(16)


class TokenReceiverHandler(http.server.BaseHTTPRequestHandler):

    # Silence the default HTTP server logging to keep terminal output clean
    def log_message(self, format, *args):
        return

    def do_GET(self):
        global AUTHORIZATION_CODE
        parsed_path = urllib.parse.urlparse(self.path)
        query_params = urllib.parse.parse_qs(parsed_path.query)

        # 1. Check for State Mismatch
        returned_state = query_params.get("state", [None])[0]
        if returned_state != STATE_KEY:
            self.send_response(400)
            self.send_header("Content-type", "text/html")
            self.end_headers()
            msg = f"<h1>Security Error</h1><p>State mismatch. Expected: {STATE_KEY}, Got: {returned_state}</p>"
            self.wfile.write(msg.encode("utf-8"))
            return

        # 2. Check for Spotify-specific API errors
        if "error" in query_params:
            error_msg = query_params["error"][0]
            self.send_response(400)
            self.send_header("Content-type", "text/html")
            self.end_headers()
            msg = f"<h1>Spotify API Error</h1><p>Reason: <b>{error_msg}</b></p>"
            self.wfile.write(msg.encode("utf-8"))
            return

        # 3. Check for successful Authorization Code
        if "code" in query_params:
            AUTHORIZATION_CODE = query_params["code"][0]
            self.send_response(200)
            self.send_header("Content-type", "text/html")
            self.end_headers()
            self.wfile.write(
                b"<h1>Success!</h1><p>You can close this window and return to the terminal.</p>"
            )
        else:
            self.send_response(400)
            self.send_header("Content-type", "text/html")
            self.end_headers()
            # Catch-all if everything else somehow passes
            self.wfile.write(
                b"<h1>Error</h1><p>Authorization code not found, but no explicit error returned.</p>"
            )


def generate_refresh_token():
    # Use the correct Spotify authorization endpoint
    auth_url = "https://accounts.spotify.com/authorize?" + urllib.parse.urlencode(
        {
            "client_id": CLIENT_ID,
            "response_type": "code",
            "redirect_uri": REDIRECT_URI,
            "scope": SCOPES,
            "state": STATE_KEY,
            # Uncomment the next line if you always want Spotify to show the login dialog
            # "show_dialog": "true",
        }
    )

    print("Opening browser for Spotify authentication...")
    webbrowser.open(auth_url)

    server_address = ("127.0.0.1", 8080)
    httpd = http.server.HTTPServer(server_address, TokenReceiverHandler)
    print("Waiting for authorization callback on http://127.0.0.1:8080 ...")

    while AUTHORIZATION_CODE is None:
        try:
            httpd.handle_request()
        except KeyboardInterrupt:
            break

        if AUTHORIZATION_CODE:
            print("\nExchanging authorization code for tokens...")
            # Crucial: Ensure this URL matches EXACTLY
            token_url = "https://accounts.spotify.com/api/token"

            # Explicitly strip any accidental newlines or trailing whitespaces from your credentials
            client_id_clean = CLIENT_ID.strip()
            client_secret_clean = CLIENT_SECRET.strip()

            # Generate base64 headers accurately
            auth_string = f"{client_id_clean}:{client_secret_clean}"
            auth_bytes = auth_string.encode("utf-8")
            auth_base64 = base64.b64encode(auth_bytes).decode("utf-8")

            # Explicitly state Content-Type for urlencoded payloads
            headers = {
                "Authorization": f"Basic {auth_base64}",
                "Content-Type": "application/x-www-form-urlencoded",
            }

            # Data structure for authorization_code exchange payload
            data = {
                "grant_type": "authorization_code",
                "code": AUTHORIZATION_CODE.strip(),
                "redirect_uri": REDIRECT_URI,
            }

            # Perform the POST request explicitly
            response = requests.post(token_url, headers=headers, data=data)

            print(f"Server response status code: {response.status_code}")

            # Trap HTML responses before json() parsing crashes the script
            if "text/html" in response.headers.get("Content-Type", ""):
                print("\n=== SYSTEM ERROR ===")
                print("Spotify returned an HTML page instead of API data.")
                print("This means the token URL is being accessed incorrectly.")
                print(
                    "Check that you haven't swapped CLIENT_ID and CLIENT_SECRET by accident."
                )
                return

            try:
                token_data = response.json()
                if response.status_code == 200:
                    print("\n=== REFRESH TOKEN GENERATED ===")
                    print(f"Refresh Token: {token_data.get('refresh_token')}")
                    print("\n=== ACCESS TOKEN ===")
                    print(f"Access Token: {token_data.get('access_token')}")
                else:
                    print(f"\nAPI Error Data: {token_data}")
            except ValueError as e:
                print(f"\nFailed to parse JSON response: {e}")


if __name__ == "__main__":
    generate_refresh_token()
