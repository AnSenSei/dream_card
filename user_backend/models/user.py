class Address(BaseModel):
    name: str
    street1: str
    city: str
    state: str
    zip_code: str
    country: str
    phone: Optional[str] = None
    email: Optional[str] = None
    street2: Optional[str] = None
    validated: Optional[bool] = False
    validation_messages: Optional[List[str]] = []
