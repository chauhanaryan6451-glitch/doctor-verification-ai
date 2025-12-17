import re

class ConfidenceScorer:
    def __init__(self):
        self.weights = {
            "npi": 0.35,
            "license": 0.20,
            "source": 0.15,
            "contact": 0.15,
            "assets": 0.15
        }

    def evaluate(self, profile):
        """
        Returns tuple: (float_score, details_dict)
        """
        score = 0.0
        details = {}

        # 1. NPI Logic
        npi = str(profile.get('npi_id', '') or profile.get('npi', ''))
        clean_npi = re.sub(r'\D', '', npi)
        if len(clean_npi) == 10:
            score += self.weights['npi']
            details['npi'] = True

        # 2. License Logic
        if profile.get('license_id') and profile.get('license_id') != 'N/A':
            score += self.weights['license']
            details['license'] = True

        # 3. Source Logic
        url = profile.get('source_url', '').lower()
        if any(x in url for x in ['.gov', 'health.usnews', 'npidb']):
            score += self.weights['source']
            details['source_trust'] = True

        # 4. Assets Logic
        if profile.get('verified_assets'):
            score += self.weights['assets']
            details['assets_verified'] = True
        elif profile.get('assets', {}).get('documents'):
            score += 0.05 # Small bonus for just having unverified docs

        return round(min(score, 1.0), 2), details
