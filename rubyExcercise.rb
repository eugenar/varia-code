require 'date'

# Computes the monthly charge for a given subscription.
#
# @return [Integer] The total monthly bill for the customer in cents, rounded
# to the nearest cent. For example, a bill of $20.00 should return 2000.
# If there are no active users or the subscription is nil, returns 0.
#
# @param [String] month - Always present
#   Has the following structure:
#   "2022-04"  // April 2022 in YYYY-MM format
#
# @param [Hash] subscription - May be nil
#   If present, has the following structure:
#   {
#     id: 763,
#     customer_id: 328,
#     monthly_price_in_cents: 359  # price per active user per month
#   }
#
# @param [Array] users - May be empty, but not nil
#   Has the following structure:
#   [
#     {
#       id: 1,
#       name: "Employee #1",
#       customer_id: 1,
#   
#       # when this user started
#       activated_on: Date.new(2021, 11, 4),
#   
#       # last day to bill for user
#       # should bill up to and including this date
#       # since user had some access on this date
#       deactivated_on: Date.new(2022, 4, 10)
#     },
#     {
#       id: 2,
#       name: "Employee #2",
#       customer_id: 1,
#   
#       # when this user started
#       activated_on: Date.new(2021, 12, 4),
#   
#       # hasn't been deactivated yet
#       deactivated_on: nil
#     },
#   ]
def monthly_charge(month, subscription, users)
  amount = 0
  if subscription.nil? return amount;

  month_date = Date.strptime(month, "%Y-%m")
  first_day = first_day_of_month(month_date)
  last_day = last_day_of_month(month_date)
  days_in_month = (last_day - first_day + 1).to_i

  users.each do |user|
    if user[‘customer_id’] == subscription[‘customer_id’]
      start_date = [user[‘activated_on’], first_day].max
      end_date = [user[‘deactivated_on’] || last_day, last_day].min
      if start_date <= end_date
        days_count = (end_date - start_date + 1).to_i
        days_ratio = [days_count.to_f / days_in_month, 1].min
        amount += subscription.monthly_price_in_cents * days_ratio
      end
    end
  end

  return amount.round;
end

####################
# Helper functions #
####################

# Takes a Date object and returns a Date which is the first day
# of that month. For example:
#
# first_day_of_month(Date.new(2022, 3, 17)) # => Date.new(2022, 3, 1)
#
# Input type: Date
# Output type: Date
def first_day_of_month(date)
  Date.new(date.year, date.month)
end

# Takes a Date object and returns a Date which is the last day
# of that month. For example:
#
# last_day_of_month(Date.new(2022, 3, 17)) # => Date.new(2022, 3, 31)
#
# Input type: Date
# Output type: Date
def last_day_of_month(date)
  Date.new(date.year, date.month, -1)
end

# Takes a Date object and returns a Date which is the next day.
# For example:
#
# next_day(Date.new(2022, 3, 17)) # => Date.new(2022, 3, 18)
# next_day(Date.new(2022, 3, 31)) # => Date.new(2022, 4, 1)
#
# Input type: Date
# Output type: Date
def next_day(date)
  date.next_day
end