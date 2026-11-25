CREATE TOOL `expedite_material_tool`
USING FUNCTION `expedite_material`
WITH (
  'type' = 'function',
  'description' = 'Raise an ERP expedite (hot-list, priority freight) for a material that is short at the line. Arguments: material_id, quantity (the shortfall, as text), reason.'
);
