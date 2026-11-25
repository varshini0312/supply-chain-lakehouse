"""Environment-aware configuration for Unity Catalog names and storage paths."""
from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    env: str = "dev"
    storage_account: str = "<storage-account>"

    @property
    def catalog(self) -> str:
        return f"supply_chain_{self.env}"

    @property
    def landing_path(self) -> str:
        return f"abfss://landing@{self.storage_account}.dfs.core.windows.net"

    def table(self, layer: str, name: str) -> str:
        return f"{self.catalog}.{layer}.{name}"


# Primary keys per entity, used by the generic CDC merge
PRIMARY_KEYS = {
    "supplier": ["supplier_id"],
    "product": ["product_id"],
    "warehouse": ["warehouse_id"],
    "inventory": ["inventory_id"],
    "purchase_order": ["po_id"],
    "customer_order": ["order_id"],
    "shipment": ["shipment_id"],
}
