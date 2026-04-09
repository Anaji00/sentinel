from services.correlation.rules.maritime     import rule_vessel_dark
from services.correlation.rules.financial    import rule_options_geo
from services.correlation.rules.aviation     import rule_emergency_squawk
from services.correlation.rules.news         import rule_sanctions_headline
from services.correlation.rules.cross_domain import rule_cross_domain_anomaly
 
ALL_RULES = [
    rule_vessel_dark,
    rule_options_geo,
    rule_emergency_squawk,
    rule_sanctions_headline,
    rule_cross_domain_anomaly,
]
 