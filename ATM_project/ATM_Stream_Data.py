import json
import random
import time
import requests
from datetime import datetime, timedelta
from faker import Faker
import uuid
import threading
import logging

fake = Faker()

class ATMTransactionGenerator:
    def __init__(self, nifi_endpoint="http://localhost:8080/contentListener"):
        self.nifi_endpoint = nifi_endpoint
        self.transaction_types = ['WITHDRAWAL', 'DEPOSIT', 'BALANCE_INQUIRY', 'TRANSFER', 'PIN_CHANGE']
        self.transaction_status = ['SUCCESS', 'FAILED', 'TIMEOUT', 'CANCELLED']
        self.atm_locations = self._generate_atm_locations(100)
        self.customer_profiles = self._generate_customer_profiles(1000)
        self._setup_logging()

    def _setup_logging(self):
        """Configure logging for the application"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler('atm_transactions.log')
            ]
        )
        self.logger = logging.getLogger(__name__)

    def _generate_atm_locations(self, count):
        """Generate ATM location data with enhanced metrics"""
        return [
            {
                'atm_id': f'ATM_{i:04d}', 
                'location': fake.address(), 
                'city': fake.city(), 
                'state': fake.state(), 
                'country': fake.country(), 
                'bank_name': fake.company(),
                'atm_type': random.choice(['Drive-through', 'Walk-up', 'Indoor', 'Mall']),
                'region': random.choice(['North', 'South', 'East', 'West', 'Central']),
                # Enhanced ATM metrics
                'atm_cash_capacity': random.randint(50000, 200000),
                'daily_transaction_limit': random.choice([500, 1000, 1500, 2000, 2500]),
                'installation_date': fake.date_between(start_date='-10y', end_date='-1y'),
                'maintenance_score': round(random.uniform(7.0, 10.0), 1),
                'uptime_percentage': round(random.uniform(95.0, 99.9), 2),
                'avg_queue_time_minutes': round(random.uniform(1.0, 5.0), 1)
            } 
            for i in range(1, count + 1)
        ]

    def _generate_customer_profiles(self, count):
        """Generate customer profiles with enhanced financial metrics"""
        profiles = []
        for i in range(count):
            # Generate consistent financial profile
            age_group = random.choice(['18-25', '26-35', '36-45', '46-55', '56-65', '65+'])
            segment = random.choice(['Premium', 'Gold', 'Silver', 'Basic'])
            
            # Age-based income ranges
            income_ranges = {
                '18-25': (25000, 45000),
                '26-35': (35000, 75000),
                '36-45': (45000, 95000),
                '46-55': (50000, 120000),
                '56-65': (45000, 100000),
                '65+': (30000, 60000)
            }
            
            # Segment-based multipliers
            segment_multipliers = {
                'Premium': (1.5, 3.0),
                'Gold': (1.2, 2.0),
                'Silver': (0.9, 1.3),
                'Basic': (0.6, 1.0)
            }
            
            base_income = random.randint(*income_ranges[age_group])
            multiplier = random.uniform(*segment_multipliers[segment])
            annual_income = int(base_income * multiplier)
            
            profile = {
                'customer_id': f'CUST_{i:06d}',
                'account_number': f'ACC_{random.randint(1000000000, 9999999999)}',
                'customer_name': fake.name(),
                'age_group': age_group,
                'customer_segment': segment,
                'account_type': random.choice(['Savings', 'Checking', 'Business', 'Student']),
                'registration_date': fake.date_between(start_date='-5y', end_date='today'),
                'city': fake.city(),
                'state': fake.state(),
                # Enhanced customer financial metrics
                'annual_income': annual_income,
                'account_balance': round(random.uniform(500, annual_income * 0.3), 2),
                'credit_score': random.randint(300, 850),
                'monthly_transactions': random.randint(5, 50),
                'avg_transaction_amount': round(random.uniform(50, 500), 2),
                'account_age_months': random.randint(1, 60),
                'overdraft_limit': random.choice([0, 100, 250, 500, 1000]),
                'loyalty_points': random.randint(0, 10000),
                'digital_banking_user': random.choice([True, False]),
                'preferred_transaction_time': random.choice(['Morning', 'Afternoon', 'Evening', 'Night'])
            }
            profiles.append(profile)
        return profiles
    
    def generate_transaction(self):
        """Generate a single ATM transaction with enhanced analytics data"""
        customer = random.choice(self.customer_profiles)
        atm = random.choice(self.atm_locations)
        transaction_type = random.choice(self.transaction_types)
        
        # Generate varied date range (last 2 years to ensure different quarters/years)
        days_back = random.randint(0, 730)  # 0 to 2 years back
        base_time = datetime.now() - timedelta(days=days_back)
        
        # Add time variation within the day
        transaction_time = base_time.replace(
            hour=random.randint(0, 23),
            minute=random.randint(0, 59),
            second=random.randint(0, 59),
            microsecond=0
        )
        
        # Enhanced amount logic based on customer profile and transaction type
        if transaction_type == 'WITHDRAWAL':
            # Amount influenced by customer segment and account balance
            segment_limits = {
                'Premium': [50, 100, 200, 500, 1000, 1500],
                'Gold': [20, 50, 100, 200, 500, 1000],
                'Silver': [20, 40, 80, 100, 200, 400],
                'Basic': [20, 40, 60, 100, 200]
            }
            possible_amounts = segment_limits.get(customer['customer_segment'], [20, 40, 60, 100])
            max_allowed = min(atm['daily_transaction_limit'], customer['account_balance'] * 0.3)
            valid_amounts = [amt for amt in possible_amounts if amt <= max_allowed]
            amount = random.choice(valid_amounts) if valid_amounts else 20
            
        elif transaction_type == 'DEPOSIT':
            # Deposits vary by income level and account type
            income_factor = customer['annual_income'] / 50000
            base_deposit = random.uniform(50, 1000) * income_factor
            amount = round(min(base_deposit, 5000), 2)
            
        elif transaction_type == 'TRANSFER':
            # Transfers based on customer segment
            segment_transfer_ranges = {
                'Premium': (500, 5000),
                'Gold': (200, 3000),
                'Silver': (100, 2000),
                'Basic': (50, 1000)
            }
            min_amt, max_amt = segment_transfer_ranges.get(customer['customer_segment'], (50, 1000))
            amount = round(random.uniform(min_amt, max_amt), 2)
        else:
            amount = 0
        
        # Enhanced status logic
        status_weights = {'SUCCESS': 0.85, 'FAILED': 0.10, 'TIMEOUT': 0.03, 'CANCELLED': 0.02}
        
        # Adjust success rate based on ATM maintenance score
        if atm['maintenance_score'] < 8.0:
            status_weights['SUCCESS'] = 0.75
            status_weights['FAILED'] = 0.20
        
        status = random.choices(list(status_weights.keys()), weights=list(status_weights.values()))[0]
        
        # Enhanced fee calculation
        if transaction_type == 'WITHDRAWAL' and status == 'SUCCESS':
            is_own_bank = random.choice([True, False])  # Simplified assumption
            if not is_own_bank:
                # Fee varies by customer segment
                segment_fees = {
                    'Premium': 0,  # Premium customers get fee waivers
                    'Gold': 1.50,
                    'Silver': 2.50,
                    'Basic': 3.50
                }
                transaction_fee = segment_fees.get(customer['customer_segment'], 2.50)
            else:
                transaction_fee = 0
        else:
            transaction_fee = 0
        
        # Calculate distance and travel metrics (simulated)
        distance_from_home = round(random.uniform(0.5, 25.0), 1)
        is_frequent_location = distance_from_home < 5.0 and random.random() < 0.7
        
        # Assign response_time_ms before using it in the transaction dictionary
        response_time_ms = random.randint(500, 5000) if status != 'TIMEOUT' else random.randint(10000, 30000)
        transaction = {
            # Core transaction data
            'transaction_id': str(uuid.uuid4()),
            'transaction_timestamp': transaction_time.isoformat(),
            'transaction_date': transaction_time.date().isoformat(),
            'transaction_time': transaction_time.time().isoformat(),
            'amount': amount,
            'transaction_fee': transaction_fee,
            'status': status,
            'response_time_ms': response_time_ms,
            'customer_id': customer['customer_id'],
            'account_number': customer['account_number'],
            'atm_id': atm['atm_id'],
            'transaction_type': transaction_type,
            
            # Enhanced time dimensions
            'year': transaction_time.year,
            'month': transaction_time.month,
            'day': transaction_time.day,
            'hour': transaction_time.hour,
            'day_of_week': transaction_time.strftime('%A'),
            'is_weekend': transaction_time.weekday() >= 5,
            'quarter': f'Q{(transaction_time.month-1)//3 + 1}',
            'week_of_year': transaction_time.isocalendar()[1],
            'days_since_epoch': (transaction_time.date() - datetime(1970, 1, 1).date()).days,
            
            # Customer information with enhanced metrics
            'customer_name': customer['customer_name'],
            'age_group': customer['age_group'],
            'customer_segment': customer['customer_segment'],
            'account_type': customer['account_type'],
            'customer_city': customer['city'],
            'customer_state': customer['state'],
            'annual_income': customer['annual_income'],
            'account_balance': customer['account_balance'],
            'credit_score': customer['credit_score'],
            'monthly_transactions': customer['monthly_transactions'],
            'avg_transaction_amount': customer['avg_transaction_amount'],
            'account_age_months': customer['account_age_months'],
            'overdraft_limit': customer['overdraft_limit'],
            'loyalty_points': customer['loyalty_points'],
            'digital_banking_user': customer['digital_banking_user'],
            'preferred_transaction_time': customer['preferred_transaction_time'],
            
            # ATM information with enhanced metrics
            'atm_location': atm['location'],
            'atm_city': atm['city'],
            'atm_state': atm['state'],
            'atm_country': atm['country'],
            'bank_name': atm['bank_name'],
            'atm_type': atm['atm_type'],
            'region': atm['region'],
            'atm_cash_capacity': atm['atm_cash_capacity'],
            'daily_transaction_limit': atm['daily_transaction_limit'],
            'maintenance_score': atm['maintenance_score'],
            'uptime_percentage': atm['uptime_percentage'],
            'avg_queue_time_minutes': atm['avg_queue_time_minutes'],
            
            # Enhanced calculated metrics
            'is_successful': status == 'SUCCESS',
            'is_high_amount': amount > 500,
            'business_hours': 9 <= transaction_time.hour <= 17,
            'is_high_value_customer': customer['customer_segment'] in ['Premium', 'Gold'],
            'transaction_to_balance_ratio': round(amount / max(customer['account_balance'], 1), 4) if amount > 0 else 0,
            'distance_from_home_km': distance_from_home,
            'is_frequent_location': is_frequent_location,
            'transaction_efficiency_score': round(10 - (response_time_ms / 500), 2),
            'customer_lifetime_value': round(customer['annual_income'] * customer['account_age_months'] / 12 * 0.02, 2),
            'transaction_velocity_risk': amount > customer['avg_transaction_amount'] * 3,
            'peak_hour_transaction': transaction_time.hour in [12, 13, 17, 18],  # Lunch and after work
            'cross_selling_opportunity': customer['digital_banking_user'] == False and customer['customer_segment'] in ['Gold', 'Premium'],
            
            # Financial ratios and scores
            'income_to_transaction_ratio': round(customer['annual_income'] / max(amount, 1), 2) if amount > 0 else 0,
            'balance_utilization_percent': round((amount / max(customer['account_balance'], 1)) * 100, 2) if amount > 0 else 0,
            'customer_risk_score': min(100, max(0, 100 - customer['credit_score'] // 8)),
            'atm_performance_index': round((atm['maintenance_score'] * atm['uptime_percentage']) / 10, 2),
            
            'created_at': datetime.now().isoformat()
        }
        
        return transaction
    
    def send_to_nifi(self, transaction):
        """Send transaction to Apache NIFI"""
        try:
            headers = {'Content-Type': 'application/json'}
            response = requests.post(
                self.nifi_endpoint,
                json=transaction,
                headers=headers,
                timeout=5
            )
            if response.status_code == 200:
                self.logger.info(f"Transaction {transaction['transaction_id'][:8]} sent successfully")
                return True
            else:
                self.logger.error(f"Failed to send transaction: HTTP {response.status_code}")
                return False
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error sending to NIFI: {e}")
            return False
    
    def generate_and_send_batch(self, batch_size=10):
        """Generate and send a batch of transactions"""
        transactions = []
        for _ in range(batch_size):
            transaction = self.generate_transaction()
            transactions.append(transaction)
            
            if self.send_to_nifi(transaction):
                time.sleep(0.1)
        
        return transactions
    
    def start_continuous_generation(self, interval_seconds=5, batch_size=1):
        """Start continuous transaction generation"""
        self.logger.info(f"Starting continuous ATM transaction generation")
        self.logger.info(f"Sending {batch_size} transaction(s) every {interval_seconds} seconds")
        self.logger.info(f"NIFI Endpoint: {self.nifi_endpoint}")
        self.logger.info("=" * 60)
        
        try:
            while True:
                self.generate_and_send_batch(batch_size)
                time.sleep(interval_seconds)
        except KeyboardInterrupt:
            self.logger.info("Stopping transaction generation")

def main():
    # Configuration
    NIFI_ENDPOINT = "http://localhost:8080/contentListener"
    BATCH_SIZE = 3
    INTERVAL_SECONDS = 0
    
    # Initialize generator
    generator = ATMTransactionGenerator(NIFI_ENDPOINT)
    
    # Test single transaction generation
    print("Testing single transaction generation:")
    sample_transaction = generator.generate_transaction()
    print(json.dumps(sample_transaction, indent=2))
    print("\n" + "=" * 60)
    
    # Start continuous generation
    generator.start_continuous_generation(INTERVAL_SECONDS, BATCH_SIZE)

if __name__ == "__main__":
    main()