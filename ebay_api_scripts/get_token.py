import requests
import xml.etree.ElementTree as ET
import webbrowser
import time

def get_ebay_production_token():
    # Your PRODUCTION credentials
    app_id = "LinusKre-DataEngi-PRD-33d821025-8e7c827e"
    cert_id = "PRD-3d82102527d4-3546-4f33-a442-5757"
    dev_id = "e9cb8e5f-41f7-4a38-9850-691d6430f901"
    ru_name = "Linus_Kr_er-LinusKre-DataEn-cnjevraew"
    
    print("🚀 PRODUCTION Environment")
    print("=" * 50)
    print(f"App ID: {app_id}")
    print(f"RuName: {ru_name}")
    print("=" * 50)
    
    headers = {
        'X-EBAY-API-SITEID': '0',  # 0 = US
        'X-EBAY-API-COMPATIBILITY-LEVEL': '967',
        'X-EBAY-API-CALL-NAME': 'GetSessionID',
        'X-EBAY-API-APP-NAME': app_id,
        'X-EBAY-API-DEV-NAME': dev_id,
        'X-EBAY-API-CERT-NAME': cert_id,
        'Content-Type': 'text/xml'
    }
    
    # PRODUCTION API endpoint
    session_url = "https://api.ebay.com/ws/api.dll"
    
    # Step 1: Get Session ID
    xml_request = f"""<?xml version="1.0" encoding="utf-8"?>
    <GetSessionIDRequest xmlns="urn:ebay:apis:eBLBaseComponents">
        <RuName>{ru_name}</RuName>
    </GetSessionIDRequest>"""
    
    print("📡 Step 1: Getting Session ID from eBay Production API...")
    response = requests.post(session_url, headers=headers, data=xml_request)
    
    if response.status_code == 200:
        root = ET.fromstring(response.content)
        ack = root.find('.//{urn:ebay:apis:eBLBaseComponents}Ack').text
        
        if ack == 'Success':
            session_id = root.find('.//{urn:ebay:apis:eBLBaseComponents}SessionID').text
            print(f"✅ Session ID obtained: {session_id}")
            
            # Step 2: Create PRODUCTION sign-in URL
            signin_url = f"https://signin.ebay.com/ws/eBayISAPI.dll?SignIn&runame={ru_name}&SessID={session_id}"
            
            print(f"\n🌐 Step 2: Sign in with your REAL eBay account")
            print(f"URL: {signin_url}")
            print("\n⚠️  IMPORTANT: Use your ACTUAL eBay username/password")
            print("   This is the REAL eBay site, not a test environment")
            print("\nOpening browser...")
            
            webbrowser.open(signin_url)
            
            # Step 3: Wait for user to complete sign-in
            print("\n⏳ Step 3: Complete the sign-in process in the browser")
            print("   - Enter your eBay username/password")
            print("   - Accept the permissions")
            print("   - Wait for the success page")
            input("Press Enter AFTER you've completed sign-in and accepted permissions...")
            
            # Step 4: Fetch the token
            return fetch_production_token(session_id, headers)
        else:
            print("❌ Failed to get session ID")
            # Print detailed error
            errors = root.findall('.//{urn:ebay:apis:eBLBaseComponents}Errors')
            for error in errors:
                short_msg = error.find('.//{urn:ebay:apis:eBLBaseComponents}ShortMessage')
                long_msg = error.find('.//{urn:ebay:apis:eBLBaseComponents}LongMessage')
                print(f"Error: {short_msg.text if short_msg is not None else 'N/A'}")
                print(f"Details: {long_msg.text if long_msg is not None else 'N/A'}")
            return None
    else:
        print(f"❌ HTTP Error: {response.status_code}")
        print(f"Response: {response.text}")
        return None

def fetch_production_token(session_id, headers):
    """Fetch the token after user sign-in"""
    headers['X-EBAY-API-CALL-NAME'] = 'FetchToken'
    
    fetch_token_xml = f"""<?xml version="1.0" encoding="utf-8"?>
    <FetchTokenRequest xmlns="urn:ebay:apis:eBLBaseComponents">
        <SessionID>{session_id}</SessionID>
    </FetchTokenRequest>"""
    
    session_url = "https://api.ebay.com/ws/api.dll"
    
    print("📡 Step 4: Fetching eBay Auth Token...")
    response = requests.post(session_url, headers=headers, data=fetch_token_xml)
    
    if response.status_code == 200:
        root = ET.fromstring(response.content)
        ack = root.find('.//{urn:ebay:apis:eBLBaseComponents}Ack').text
        
        if ack == 'Success':
            ebay_auth_token = root.find('.//{urn:ebay:apis:eBLBaseComponents}eBayAuthToken').text
            token_expiration = root.find('.//{urn:ebay:apis:eBLBaseComponents}HardExpirationTime').text
            
            print("🎉 SUCCESS! eBay Production Token Obtained!")
            print("=" * 50)
            print(f"🔑 eBay Auth Token: {ebay_auth_token}")
            print(f"⏰ Expires: {token_expiration}")
            print("=" * 50)
            
            # Save token to file
            with open('ebay_production_token.txt', 'w') as f:
                f.write(f"Token: {ebay_auth_token}\n")
                f.write(f"Expires: {token_expiration}\n")
                f.write(f"RuName: Linus_Kr_er-LinusKre-DataEn-cnjevraew\n")
                f.write(f"App ID: LinusKre-DataEngi-PRD-33d821025-8e7c827e\n")
            
            print("💾 Token saved to 'ebay_production_token.txt'")
            
            return ebay_auth_token
        else:
            print("❌ Failed to fetch token. Possible reasons:")
            print("   - User didn't complete sign-in")
            print("   - Session expired (took too long)")
            print("   - User declined permissions")
            
            # Print error details
            errors = root.findall('.//{urn:ebay:apis:eBLBaseComponents}Errors')
            for error in errors:
                short_msg = error.find('.//{urn:ebay:apis:eBLBaseComponents}ShortMessage')
                long_msg = error.find('.//{urn:ebay:apis:eBLBaseComponents}LongMessage')
                print(f"Error: {short_msg.text if short_msg is not None else 'N/A'}")
            
            return None
    else:
        print(f"❌ HTTP Error fetching token: {response.status_code}")
        print(f"Response: {response.text}")
        return None

# Run the production token generator
if __name__ == "__main__":
    print("eBay Production Token Generator")
    print("This will generate a token for REAL eBay production data")
    print("Make sure you're using your ACTUAL eBay account credentials")
    print()
    
    token = get_ebay_production_token()
    
    if token:
        print("\n✅ SUCCESS! You can now use this token in your API calls.")
        print("This token is valid for approximately 18 months.")
    else:
        print("\n❌ Failed to get token. Please check:")
        print("   - Your eBay username/password is correct")
        print("   - You accepted the permissions")
        print("   - You completed the sign-in within 2-3 minutes")