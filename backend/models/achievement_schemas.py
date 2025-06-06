from typing import List, Dict, Any, Optional, Union
from pydantic import BaseModel, Field


class ConditionSchema(BaseModel):
    """Schema for achievement condition"""
    type: str
    target: int


class PointRewardSchema(BaseModel):
    """Schema for point reward"""
    type: str = "point"
    amount: int


class EmblemRewardSchema(BaseModel):
    """Schema for emblem reward"""
    type: str = "emblem"
    emblemId: str
    url: str


class EmblemRewardInputSchema(BaseModel):
    """Schema for emblem reward input"""
    type: str = "emblem"
    image: Optional[str] = None  # Base64 encoded image


class AchievementCreate(BaseModel):
    """Schema for creating an achievement"""
    name: str
    description: str
    condition: ConditionSchema
    reward: List[Union[PointRewardSchema, EmblemRewardInputSchema]]


class UploadAchievementSchema(BaseModel):
    """Schema for uploading an achievement with base64 image"""
    name: str
    description: str
    condition: Dict[str, Any]  # {"type": string, "target": number}
    reward: List[Dict[str, Any]]  # List of reward objects
    rarity: Optional[str] = None  # e.g., "common", "rare", "epic", "legendary"
    rank: Optional[int] = None  # e.g., 1, 2, 3, etc.


class Achievement(BaseModel):
    """Schema for an achievement stored in the database"""
    id: str
    name: str
    description: str
    condition: ConditionSchema
    reward: List[Union[PointRewardSchema, EmblemRewardSchema]]
    rarity: Optional[str] = None
    rank: Optional[int] = None


class AchievementResponse(BaseModel):
    """Schema for achievement response"""
    id: str
    name: str
    description: str
    condition: ConditionSchema
    reward: List[Union[PointRewardSchema, EmblemRewardSchema]]
    rarity: Optional[str] = None
    rank: Optional[int] = None


class AchievementCreateForm(BaseModel):
    """Schema for creating an achievement via form data"""
    id: str
    name: str
    description: str
    condition: str  # JSON string
    reward: str  # JSON string


class PaginatedAchievementResponse(BaseModel):
    """Schema for paginated achievement list response"""
    items: List[AchievementResponse]
    total: int
    page: int
    size: int
