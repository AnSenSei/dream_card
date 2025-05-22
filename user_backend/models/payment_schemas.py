from pydantic import BaseModel, Field
from typing import Optional, Dict, Any
from datetime import datetime

class CreatePaymentIntentRequest(BaseModel):
    """
    Request model for creating a payment intent.
    """
    amount: int = Field(..., description="Amount to charge in cents (e.g., 1000 for $10.00)")
    currency: str = Field("usd", description="Currency code (default: usd)")
    metadata: Optional[Dict[str, Any]] = Field(None, description="Additional metadata for the payment intent")

class PaymentIntentResponse(BaseModel):
    """
    Response model for a payment intent.
    """
    id: str = Field(..., description="Stripe payment intent ID")
    client_secret: str = Field(..., description="Client secret used to complete the payment on the client side")
    amount: int = Field(..., description="Amount in cents")
    currency: str = Field(..., description="Currency code")
    status: str = Field(..., description="Payment intent status")
    created_at: Optional[datetime] = Field(None, description="Creation timestamp")

class WebhookResponse(BaseModel):
    """
    Response model for webhook events.
    """
    status: str = Field(..., description="Status of the webhook processing (success, ignored, etc.)")
    details: Dict[str, Any] = Field(..., description="Details about the processed webhook event")
