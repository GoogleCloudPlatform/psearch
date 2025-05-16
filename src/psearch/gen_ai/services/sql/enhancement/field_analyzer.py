#
# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import re
from typing import Dict, Any, Optional, List

logger = logging.getLogger(__name__)

class FieldAnalyzer:
    """
    Analyzes SQL queries and schemas to identify fields for semantic enhancement
    and find potential matches.
    """

    # Fields that should be prioritized for semantic mapping when source fields don't match directly
    # Moved from SQLTransformationService.CRITICAL_FIELDS
    DEFAULT_CRITICAL_FIELDS = [
        "id",                  # Primary identifier
        "name",                # Product name
        "title",               # Alternative title/name
        "description",         # Product description
        "images",              # Product images
        "categories",          # Product categories
        "brands",              # Brand information
        "priceInfo.price",     # Product price (nested field)
        "priceInfo.currencyCode"  # Currency (nested field)
    ]

    def identify_defaulted_fields(
        self,
        sql_query: str,
        critical_fields: Optional[List[str]] = None
    ) -> List[str]:
        """
        Identifies which critical fields were given default values in the SQL.
        (Logic from SQLTransformationService.identify_defaulted_fields)

        Args:
            sql_query: The generated SQL query string.
            critical_fields: Optional list of critical field names to check. 
                             Uses DEFAULT_CRITICAL_FIELDS if None.

        Returns:
            A list of critical field names that appear to have been defaulted.
        """
        if critical_fields is None:
            critical_fields_to_check = self.DEFAULT_CRITICAL_FIELDS
        else:
            critical_fields_to_check = critical_fields
            
        fields_to_refine = []
        if not sql_query:
            logger.warning("SQL query is empty, cannot identify defaulted fields.")
            return fields_to_refine

        sql_query_lowered = sql_query.lower()
        
        for field in critical_fields_to_check:
            field_lower = field.lower() # Ensure field name is also lower for matching
            # logger.debug(f"Checking critical field for default: {field}")
            if "." in field: # Nested fields like priceInfo.price
                parent, child = field_lower.split(".", 1)
                
                # Patterns for STRUCT( <default_value> AS child_field_name ... )
                # Example: STRUCT(NULL AS price ...), STRUCT(0 AS price ...)
                # We need to be careful with regex here to match the child field within a STRUCT
                # A simpler string search might be more robust if the structure is consistent.
                # The original code used string searching which is less prone to complex regex issues.
                
                # Search for patterns like:
                # `STRUCT(NULL AS child`
                # `STRUCT(0 AS child`
                # `STRUCT(FALSE AS child`
                # `STRUCT([] AS child`
                # `STRUCT('' AS child`
                # `STRUCT("" AS child`
                # These checks are simplified; more robust parsing might be needed for complex STRUCTs.
                # The key is that the `child` part of `parent.child` is being defaulted.
                
                # We look for `AS child_field_lower` preceded by a default value within a STRUCT context.
                # This is a heuristic. A full SQL parser would be more accurate but complex.
                struct_patterns = [
                    rf"struct\s*\((?:[^()]*,\s*)*?(?:null|0|false|\[\]|''|\"\"|\{{\}})(?:\s+as\s+`?{re.escape(child)}`?)\b", # Match default AS child
                    rf"struct\s*\((?:[^()]*,\s*)*?(?:`?{re.escape(child)}`?\s*:\s*(?:null|0|false|\[\]|''|\"\"|\{{\}}))\b" # Match child : default (less common in BQ SELECT)
                ]
                # Simpler string checks from original code (adapted)
                simple_struct_checks = [
                    f"struct(" + f"null as {child}",
                    f"struct(" + f"0 as {child}",
                    f"struct(" + f"false as {child}",
                    f"struct(" + f"[] as {child}",
                    f"struct(" + f"{{}} as {child}", # {} is not a valid BQ default for struct, but checking
                    f"struct(" + f"'' as {child}",
                    f"struct(" + f"\"\" as {child}",
                ]

                found_default = False
                for pattern_text in simple_struct_checks:
                    if pattern_text in sql_query_lowered: # Check if `parent_field.child_field` is defaulted
                        # This check needs to be more specific to ensure it's `parent.child`
                        # A more robust check would involve looking for `parent_field AS STRUCT(... child AS DEFAULT ...)`
                        # For now, if any of these simple patterns for the child are found, we assume it might be defaulted.
                        # This is an approximation.
                        # A more precise check would be: `parent_field AS STRUCT(..., child AS <default>, ...)`
                        # Or `STRUCT(... child AS <default> ...) AS parent_field`
                        # The original logic was simpler and might be sufficient as a heuristic.
                        # Let's refine to check for `AS parent_lower` followed by struct containing defaulted child
                        
                        # Heuristic: if `AS parent_name` is followed by a struct where `child_name` is defaulted.
                        # Example: ... STRUCT(NULL AS price) AS priceinfo ...
                        # This is still tricky without full parsing.
                        # The original code's simple string search for `STRUCT( <default> AS child` is a broad heuristic.
                        # If `parent.child` is critical, and `child` is defaulted inside *any* struct, it's flagged.
                        # This might lead to false positives if `child` name is common.
                        
                        # Let's stick to the original simpler logic for now, as it's a heuristic.
                        # If `STRUCT(... NULL AS child ...)` exists anywhere, and `parent.child` is critical, flag it.
                        found_default = True
                        break
                
                if found_default:
                    fields_to_refine.append(field)
                    # logger.debug(f"  -> Nested field '{field}' potentially defaulted.")

            else: # Regular (non-nested) fields
                # Patterns for `DEFAULT_VALUE AS field_name`
                # Example: `NULL AS description`, `0 AS quantity`
                # Using simple string search from original code for robustness against SQL variations
                simple_direct_checks = [
                    f"null as {field_lower}",
                    f"0 as {field_lower}",
                    f"false as {field_lower}",
                    f"[] as {field_lower}",
                    f"{{}} as {field_lower}", # {} is not a valid BQ default for struct, but checking
                    f"'' as {field_lower}",
                    f"\"\" as {field_lower}",
                ]
                for pattern_text in simple_direct_checks:
                     # Ensure it's `AS field_lower` and not part of another word. Add word boundaries.
                    if re.search(r"\b" + re.escape(pattern_text) + r"\b", sql_query_lowered):
                        fields_to_refine.append(field)
                        # logger.debug(f"  -> Direct field '{field}' potentially defaulted.")
                        break
        
        unique_fields_to_refine = list(set(fields_to_refine))
        if unique_fields_to_refine:
             logger.info(f"Identified {len(unique_fields_to_refine)} critical fields potentially defaulted: {unique_fields_to_refine}")
        return unique_fields_to_refine

    def analyze_source_fields_for_semantic_matches(
        self,
        source_schema_fields: List[str]
    ) -> Dict[str, List[str]]:
        """
        Analyzes source fields to identify potential semantic matches for common critical destination fields.
        (Logic from SQLTransformationService.analyze_source_fields)

        Args:
            source_schema_fields: List of available field names in the source.

        Returns:
            A dictionary where keys are common critical field concepts (e.g., "id", "name")
            and values are lists of source fields that are potential semantic matches.
        """
        semantic_match_candidates: Dict[str, List[str]] = {}
        
        # Common field name patterns for important attributes
        # These patterns are designed to match parts of words or common variations.
        patterns = {
            "id": [r"id$", r"ident", r"key", r"code", r"sku", r"product.?id", r"item.?id", r"uuid"],
            "name": [r"name$", r"title", r"label", r"product.?name", r"item.?name", r"heading"],
            "description": [r"desc", r"detail", r"summary", r"text", r"abstract", r"notes", r"comment"],
            "price": [r"price", r"cost", r"amount", r"value", r"charge", r"fee", r"rate"],
            "image": [r"image", r"img", r"picture", r"photo", r"thumb", r"url", r"graphic", r"icon"],
            "category": [r"categor", r"group", r"type", r"class", r"department", r"section"],
            "brand": [r"brand", r"manufacturer", r"vendor", r"make", r"company", r"label"], # "label" can be ambiguous
            "currency": [r"currency", r"ccy", r"curr.?code"],
            # Add more critical concepts as needed
        }
        
        logger.debug(f"Analyzing {len(source_schema_fields)} source fields for semantic matches.")
        for source_field in source_schema_fields:
            source_field_lower = source_field.lower()
            for critical_concept, field_patterns in patterns.items():
                if any(re.search(pattern, source_field_lower, re.IGNORECASE) for pattern in field_patterns):
                    if critical_concept not in semantic_match_candidates:
                        semantic_match_candidates[critical_concept] = []
                    if source_field not in semantic_match_candidates[critical_concept]: # Avoid duplicates
                        semantic_match_candidates[critical_concept].append(source_field)
                        # logger.debug(f"  Found potential match for '{critical_concept}': '{source_field}'")
    
        if semantic_match_candidates:
            logger.info(f"Found potential semantic match candidates: {json.dumps(semantic_match_candidates, indent=2)}")
        return semantic_match_candidates

    def select_best_field_matches(
        self,
        source_fields: List[str],
        destination_fields_to_match: List[str],
        # source_data_sample: Optional[List[Dict[str, Any]]] = None # Placeholder for future content-based validation
    ) -> Dict[str, str]:
        """
        Selects the best source field for each destination field using a multi-tier approach.
        (Logic from SQLTransformationService.select_best_field_matches)

        Args:
            source_fields: List of available source field names.
            destination_fields_to_match: List of destination field names to find matches for.
            # source_data_sample: Optional sample of source data for content-based validation (future).

        Returns:
            A dict mapping destination fields to the best-matched source fields.
        """
        matches: Dict[str, str] = {}
        # Create a mapping of lowercase source fields to original casing for easier lookup
        source_fields_map_lower_to_original = {f.lower(): f for f in source_fields}
        
        # Track used source fields to avoid re-mapping the same source field to multiple destinations
        # unless absolutely necessary (e.g. if destination fields are synonyms of each other).
        # For now, let's allow a source field to be mapped multiple times if it's the best match.
        # A more sophisticated version could rank matches and ensure exclusivity if needed.

        # Step 1: Exact case-insensitive matches
        for dest_field in destination_fields_to_match:
            dest_field_lower = dest_field.lower()
            if dest_field_lower in source_fields_map_lower_to_original:
                matches[dest_field] = source_fields_map_lower_to_original[dest_field_lower]
                # logger.debug(f"Exact match for '{dest_field}': '{matches[dest_field]}'")
        
        # Step 2: For unmatched fields, try pattern-based semantic matching
        unmatched_dest_fields = [df for df in destination_fields_to_match if df not in matches]
        
        if unmatched_dest_fields:
            # Get potential semantic matches based on patterns
            # This uses the more generic "critical concepts" from analyze_source_fields_for_semantic_matches
            semantic_candidates_by_concept = self.analyze_source_fields_for_semantic_matches(source_fields)
            
            for dest_field in unmatched_dest_fields:
                dest_field_lower = dest_field.lower()
                best_candidate_for_dest: Optional[str] = None

                # Try to match dest_field directly to a critical concept
                # (e.g. if dest_field is 'product_name', it might match 'name' concept)
                # This requires mapping dest_field to a concept first.
                # For simplicity, let's assume dest_field itself is a concept or part of one.
                
                # A simple approach: iterate through concepts, if dest_field matches a pattern for that concept,
                # then pick the first available candidate source field for that concept.
                # This is a basic heuristic.
                
                # Example: if dest_field is "productId", concept is "id".
                # Look for "id" in semantic_candidates_by_concept.
                # If dest_field is "priceInfo.price", concept is "price".
                
                target_concept = dest_field_lower
                if "." in dest_field_lower: # For "parent.child", focus on "child" or "parentchild"
                    parent, child = dest_field_lower.split(".",1)
                    # Try matching child first, then parent, then combined.
                    # This logic can be quite complex. For now, let's simplify.
                    # The original `select_best_field_matches` used `candidates[child][0]`
                    # which implies `analyze_source_fields` was keyed by destination-like names.
                    # The current `analyze_source_fields_for_semantic_matches` is keyed by generic concepts.
                    
                    # Let's try to find a concept that dest_field (or its parts) relate to.
                    # This part needs refinement to bridge `destination_fields_to_match` with `semantic_candidates_by_concept` keys.
                    
                    # Simplified: if dest_field (or its child part) is a key in semantic_candidates_by_concept
                    # or if a pattern for a concept matches dest_field.
                    
                    # For "priceInfo.price", we'd ideally look for "price" concept.
                    # Let's assume for now `destination_fields_to_match` are like the concepts.
                    concept_to_check = child if "." in dest_field else dest_field
                    
                    if concept_to_check in semantic_candidates_by_concept and semantic_candidates_by_concept[concept_to_check]:
                        # Prefer candidates not already used in exact matches, if possible.
                        # This is a simple way to try and get unique mappings.
                        available_candidates = [
                            c for c in semantic_candidates_by_concept[concept_to_check] 
                            if c not in matches.values()
                        ]
                        if available_candidates:
                            best_candidate_for_dest = available_candidates[0]
                        else: # All candidates for this concept are already used, pick the first one anyway
                            best_candidate_for_dest = semantic_candidates_by_concept[concept_to_check][0]
                
                elif dest_field_lower in semantic_candidates_by_concept and semantic_candidates_by_concept[dest_field_lower]:
                    available_candidates = [
                        c for c in semantic_candidates_by_concept[dest_field_lower]
                        if c not in matches.values()
                    ]
                    if available_candidates:
                        best_candidate_for_dest = available_candidates[0]
                    else:
                        best_candidate_for_dest = semantic_candidates_by_concept[dest_field_lower][0]

                if best_candidate_for_dest:
                    matches[dest_field] = best_candidate_for_dest
                    # logger.debug(f"Semantic match for '{dest_field}': '{matches[dest_field]}'")

        # Step 3: Content-based validation (future enhancement)
        # if source_data_sample and len(source_data_sample) > 0:
        #     pass # Placeholder

        if matches:
            logger.info(f"Selected best field matches: {json.dumps(matches, indent=2)}")
        return matches


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    analyzer = FieldAnalyzer()

    # Test identify_defaulted_fields
    print("\n--- Testing identify_defaulted_fields ---")
    test_sql_1 = """
    CREATE OR REPLACE TABLE `my.dest.table` AS SELECT
      source.id AS id,
      NULL AS name, -- Defaulted name to NULL as no direct source match found.
      STRUCT(
        0 AS price, -- Defaulted price to 0 as no direct source match found.
        NULL AS currencyCode -- Defaulted currencyCode to NULL as no direct source match found.
      ) AS priceInfo,
      source.desc AS description,
      [] AS categories, -- Defaulted categories to [] as no direct source match found.
      FALSE AS available, -- Defaulted available to FALSE as no direct source match found.
      source.brandName AS brands
    FROM `my.source.table` AS source
    """
    defaulted = analyzer.identify_defaulted_fields(test_sql_1)
    print(f"Defaulted fields in test_sql_1: {defaulted}") # Expected: name, priceInfo.price, priceInfo.currencyCode, categories

    test_sql_2 = "CREATE OR REPLACE TABLE `t` AS SELECT 'val' as name, 123 as id"
    defaulted_2 = analyzer.identify_defaulted_fields(test_sql_2)
    print(f"Defaulted fields in test_sql_2: {defaulted_2}") # Expected: [] (or based on other critical fields)


    # Test analyze_source_fields_for_semantic_matches
    print("\n--- Testing analyze_source_fields_for_semantic_matches ---")
    mock_source_fields_analysis = ["product_ID", "productName", "PriceAmount", "description_text", "stockQty", "categories_list", "isAvailable", "brand_name", "image_url", "mainImage"]
    semantic_matches = analyzer.analyze_source_fields_for_semantic_matches(mock_source_fields_analysis)
    # Expected: {'id': ['product_ID'], 'name': ['productName'], 'price': ['PriceAmount'], 'description': ['description_text'], 'category': ['categories_list'], 'brand': ['brand_name'], 'image': ['image_url', 'mainImage']}
    
    # Test select_best_field_matches
    print("\n--- Testing select_best_field_matches ---")
    source_for_select = ["ProductID", "Name", "ItemDesc", "Cost", "MainImageURL", "Vendor"]
    dest_for_select = ["id", "name", "description", "price", "images", "brand", "non_existent_field"]
    
    best_matches = analyzer.select_best_field_matches(source_for_select, dest_for_select)
    print(f"Best matches: {best_matches}")
    # Expected: {'id': 'ProductID', 'name': 'Name', 'description': 'ItemDesc', 'price': 'Cost', 'images': 'MainImageURL', 'brand': 'Vendor'} (non_existent_field might not be matched)
