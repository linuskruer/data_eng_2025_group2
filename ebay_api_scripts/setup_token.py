import sys
import os

# Stelle sicher, dass der ebay_api_client importiert werden kann
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from ebay_api_client import EbayAPIClient

def main():
    if len(sys.argv) != 2:
        print("Usage: python setup_token.py [AUTH_CODE]")
        print("\nTo get a new auth code:")
        client = EbayAPIClient()
        auth_url = client.get_auth_url()
        print(f"1. Open this URL: {auth_url}")
        print("2. Authorize the application")
        print("3. Copy the NEW auth code from the URL")
        print("4. Run: python setup_token.py [NEW_AUTH_CODE]")
        return
    
    auth_code = sys.argv[1]
    print(f"Setting up token with code: {auth_code}")
    
    client = EbayAPIClient()
    
    if client.setup_initial_token(auth_code):
        print("✅ Token setup completed successfully!")
        
        # Teste die Verbindung
        print("Testing connection...")
        if client.test_connection():
            print("✅ Connection test successful! API is ready to use.")
        else:
            print("❌ Connection test failed.")
    else:
        print("❌ Token setup failed!")
        print("\nPossible reasons:")
        print("- Auth code was already used")
        print("- Auth code expired (they are only valid for a few minutes)")
        print("- Invalid auth code format")
        print("\nGet a NEW auth code by visiting the URL above.")

if __name__ == "__main__":
    main()