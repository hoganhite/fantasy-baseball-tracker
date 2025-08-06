from espn_api.baseball import League

# Initialize the league with your league ID, year, and credentials
league = League(
    league_id=3438,
    year=2025,
    espn_s2='AEC2H%2BcNaIho%2BAUEv4CwdSsviRIVV1DdOZ6fVpnLh9b9XJ76nGnUu4y5j2fxNU7l2UkNo%2BuKLm0z%2FFZnE3nGREQImur%2FiGj1YjrHeYlsa7A8m110yZWNJ%2ByNVxotqQT29wbZWqlTsW4ucWCmmeOGQxzswT0C7B7QQSKKwj%2BqVAzQuiwiXmNRR65r1bCfdfJGYhqz%2BB8IbQQipISV3x3p1sZOAVHf2czMEhJNDH7NZSPuOSO5tzIbB1JE8spbKB87cwR4X56kWMHmBRKa8NGGX%2BRU20Mwu9QmGcx%2F%2BLLmlDDdDA%3D%3D',
    swid='{B0667B75-1C17-4493-9E8F-BB24C3066905}'
)

# Print all teams in the league
print("Teams in League ID 3438:")
for team in league.teams:
    if team.owners:
        owner_names = [owner.get('displayName', 'Unknown') for owner in team.owners]
    else:
        owner_names = ['Unknown']
    owner = ', '.join(owner_names)
    print(f"- {team.team_name} (Owner: {owner})")

# Print rosters for each team
print("\nRosters:")
for team in league.teams:
    print(f"\nTeam: {team.team_name}")
    for player in team.roster:
        print(f"  - {player.name} ({player.position})")
