from flask_wtf import FlaskForm
from wtforms import StringField, PasswordField, SubmitField, SelectField, IntegerField, HiddenField, DateField
from wtforms.validators import DataRequired, Length, Email, EqualTo

class RegistrationForm(FlaskForm):
    username = StringField('Username', validators=[DataRequired(), Length(min=4, max=80)])
    email = StringField('Email', validators=[DataRequired(), Email()])
    password = PasswordField('Password', validators=[DataRequired(), Length(min=8)])
    confirm_password = PasswordField('Confirm Password', validators=[DataRequired(), EqualTo('password')])
    submit = SubmitField('Register')

class LoginForm(FlaskForm):
    username = StringField('Username', validators=[DataRequired()])
    password = PasswordField('Password', validators=[DataRequired()])
    submit = SubmitField('Login')

class LinkLeagueForm(FlaskForm):
    league_id = IntegerField('League ID', validators=[DataRequired()])
    espn_s2 = StringField('ESPN S2 Cookie', validators=[DataRequired()])
    swid = StringField('SWID', validators=[DataRequired()])
    submit = SubmitField('Link League')

class ContestForm(FlaskForm):
    league_id = SelectField('Select League', coerce=int, validators=[DataRequired()])
    stat_category = SelectField('Stat Category', choices=[
        ('OBP', 'OBP'),
        ('HR', 'Home Runs'),
        ('RBI', 'RBI'),
        ('AVG', 'Batting Average'),
        ('HITS', 'Hits'),
        ('RUNS SCORED', 'Runs Scored'),
        ('WALKS', 'Walks'),
        ('STOLEN BASES', 'Stolen Bases'),
        ('SLUGGING PERCENTAGE', 'Slugging Percentage'),
        ('INNINGS PITCHED', 'Innings Pitched'),
        ('HITS ALLOWED', 'Hits Allowed'),
        ('ERA', 'ERA'),
        ('WALKS ALLOWED', 'Walks Allowed'),
        ('STRIKEOUTS', 'Strikeouts'),
        ('QUALITY STARTS', 'Quality Starts'),
        ('WINS', 'Wins'),
        ('SAVES', 'Saves'),
        ('SAVES + HOLDS', 'Saves + Holds'),
        ('WHIP', 'WHIP'),
        ('K/BB', 'K/BB')
    ], validators=[DataRequired()])
    title = StringField('Contest Title (optional)', validators=[Length(max=100)])
    start_date = DateField('Start Date', validators=[DataRequired()])
    end_date = DateField('End Date', validators=[DataRequired()])
    submit = SubmitField('Create')

class DeleteLeagueForm(FlaskForm):
    league_id = HiddenField('League ID', validators=[DataRequired()])
    submit = SubmitField('Delete')