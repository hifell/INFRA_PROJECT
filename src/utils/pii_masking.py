import pandas as pd

def mask_pii_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Utility function to mask sensitive PII (Personally Identifiable Information) data,
    such as full crypto wallet addresses or user emails, by replacing middle characters with asterisks (***).
    
    Args:
        df: Pandas DataFrame containing potential PII columns.
        
    Returns:
        Pandas DataFrame with masked PII columns.
    """
    df_masked = df.copy()
    
    # Helper function for masking email
    def mask_email(email):
        if not isinstance(email, str) or '@' not in email:
            return email
        parts = email.split('@')
        name = parts[0]
        domain = parts[1]
        if len(name) <= 2:
            masked_name = name[0] + "*" * len(name)
        else:
            masked_name = name[0] + "***" + name[-1]
        return f"{masked_name}@{domain}"

    # Helper function for masking crypto wallet address (e.g., 0x123...abc)
    def mask_wallet(address):
        if not isinstance(address, str):
            return address
        if len(address) > 10:
            return address[:6] + "***" + address[-4:]
        return address

    # Dynamically scan columns and apply masking
    for col in df_masked.columns:
        col_lower = col.lower()
        if 'email' in col_lower:
            df_masked[col] = df_masked[col].apply(mask_email)
        elif 'wallet' in col_lower or 'address' in col_lower:
            df_masked[col] = df_masked[col].apply(mask_wallet)
            
    return df_masked
