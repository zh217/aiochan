import random

from aiochan import *

# parameters. you can experiment with different values.

N_PHILOSOPHERS = 5  # number of participating philosophers. 0 means all named philosophers below.
LEAVE_AFTER_MEAL_PROB = 0  # if 0, then the program runs forever. think what wonderful philosophy can result from this!
THINK_BEFORE_MEAL_PROB = 0.6
MAX_EAT_TIME = 0  # seconds
MAX_THINK_TIME = 0  # seconds

# name setups, etc.

PHILOSPHERS = ['Adam Smith', 'Al-Ghazali', 'Alain de Benoist', 'Alan Turing', 'Alan Watts', 'Albert Camus',
               'Albertus Magnus', 'Anaximenes', 'Aristotle', 'Arthur Schopenhauer', 'Auguste Comte', 'Averroes',
               'Ayn Rand', 'Baruch Spinoza', 'Bertrand Russell', 'Cesare Beccaria', 'Charles Sanders Peirce',
               'Confucius', 'Daniel Dennett', 'David Hume', 'Democritus', 'Diogenes', 'Edmund Burke', 'Edmund Husserl',
               'Emile Durkheim', 'Emma Goldman', 'Epictetus', 'Epicurus', 'Francis Bacon', 'Friedrich Engels',
               'Friedrich Hayek', 'Friedrich Nietzsche', 'George Berkeley', 'George Herbert Mead', 'George Santayana',
               'Georges Bataille', 'Gilles Deleuze', 'Giordano Bruno', 'Gorgias', 'Gottfried Leibniz', 'Hannah Arendt',
               'Henry David Thoreau', 'Hildegard Of Bingen', 'Hypatia', 'Ibn Sina', 'Immanuel Kant', 'Iris Murdoch',
               'Jacques Derrida', 'Jacques Lacan', 'Jean Baudrillard', 'Jean Jacques Rousseau', 'Jean-Paul Sartre',
               'Jeremy Bentham', 'Jiddu Krishnamurti', 'John Dewey', 'John Hick', 'John Locke', 'John Rawls',
               'John Searle', 'John Stuart Mill', 'Jonathan Edwards', 'Judith Butler', 'Julia Kristeva',
               'Jürgen Habermas', 'Karl Barth', 'Karl Jaspers', 'Karl Marx', 'Lao Tzu', 'Lucien Goldmann',
               'Ludwig Wittgenstein', 'Marcus Aurelius', 'Mario Bunge', 'Martha Nussbaum', 'Martin Buber',
               'Martin Heidegger', 'Mary Wollstonecraft', 'Max Scheler', 'Max Stirner', 'Meister Eckhart',
               'Michel Foucault', 'Miguel De Unamuno', 'Niccolò Machiavelli', 'Nick Bostrom', 'Noam Chomsky',
               'Parmenides', 'Peter Singer', 'Plato', 'René Descartes', 'Robert Nozick', 'Roland Barthes',
               'Rosa Luxemburg', 'Rudolf Steiner', 'Seneca', 'Sigmund Freud', 'Simone De Beauvoir', 'Simone Weil',
               'Socrates', 'Sor Juana', 'Søren Kierkegaard', 'Theodor Adorno', 'Thomas Aquinas', 'Thomas Hobbes',
               'Thomas Jefferson', 'Thomas Kühn', 'U.G. Krishnamurti', 'Voltaire', 'Walter Benjamin', 'Wilhelm Reich',
               'William James', 'William Lane Craig']

HOW = ['philosophically', 'mathematically', 'mechanically', 'electrically', 'falsely', 'irreversibly',
       'lovingly', 'hatefully', 'politically', 'ideally', 'materialistically', 'teleologically', 'greenly',
       'abstractly', 'economically', 'eternally', 'egotistically', 'altruistically', 'stochastically',
       'majestically', 'randomly', 'ferociously', 'incompletely', 'naturally', 'morally', 'socially',
       'religiously', 'speculatively', 'critically', 'scholastically', 'practically', 'analytically',
       'classically', 'platonically', 'transcendentally', 'positively', 'experimentally', 'stoically',
       'liberally', 'legally', 'scientifically', 'ethically', 'existentially', 'purely', 'academically',
       'linguistically', 'theoretically', 'rationally']

random.shuffle(PHILOSPHERS)

if N_PHILOSOPHERS >= 1:
    if N_PHILOSOPHERS < len(PHILOSPHERS):
        PHILOSPHERS = PHILOSPHERS[:N_PHILOSOPHERS]
    else:
        PHILOSPHERS += [f'Unknown Philosopher #{i}' for i in range(N_PHILOSOPHERS - len(PHILOSPHERS))]


# program logic

async def think(phi_id):
    print(f'{phi_id} is philosophizing {random.choice(HOW)} ...')
    await timeout(random.uniform(0, MAX_THINK_TIME)).get()


async def eat(phi_id):
    print(f'{phi_id} is eating {random.choice(HOW)} ...')
    await timeout(random.uniform(0, MAX_EAT_TIME)).get()


async def return_forks(slots, forks):
    for slot, fork in zip(slots, forks):
        if fork:
            await slot.put(fork)


async def grab_forks(phi_id, left, right):
    left_fork, _ = await select(left, timeout(0))
    right_fork, _ = await select(right, timeout(0))

    if left_fork and right_fork:
        print(f'{phi_id} grabs {left_fork} and {right_fork}.')
        return left_fork, right_fork
    else:
        await return_forks((left, right), (left_fork, right_fork))


async def dining_philosopher(phi_id, left, right, diner):
    print(f'{phi_id} sits down.')
    n_meals = 0
    n_blocks = 0
    while True:
        if random.random() < THINK_BEFORE_MEAL_PROB:
            await think(phi_id)

        forks = await grab_forks(phi_id, left, right)
        if forks:
            await eat(phi_id)
            n_meals += 1
            print(f'{phi_id} finishes eating and puts down {forks[0]} and {forks[1]}. '
                  f'({n_meals} meal(s) and {n_blocks} fail(s) so far)')
            await return_forks([left, right], forks)
            if random.random() < LEAVE_AFTER_MEAL_PROB:
                await diner.put(phi_id)
                break
        else:
            n_blocks += 1
            print(f'{phi_id} cannot grab both forks. ({n_meals} meal(s) and {n_blocks} fail(s) so far)')


async def dining_manager(n, diner):
    n_left = 0
    print(f'\tThe philosophers are feasting!')
    async for phi_id in diner:
        print(f'{phi_id} is content and leaves {random.choice(HOW)}.')
        n_left += 1
        print(f'\t{n_left} philosopher(s) served, {n - n_left} remaining.')
        if n_left == n:
            print(f'\tAll philosophers are gone, closing down.')
            diner.close()


async def start_dining(philosophers):
    philosophers = list(philosophers)
    n_philosophers = len(philosophers)
    forks = [Chan(1).add(f'fork {i}') for i in range(n_philosophers)]
    diner = Chan()
    for idx in range(n_philosophers):
        phi_id = philosophers[idx]
        left = forks[idx]
        right = forks[(idx + 1) % n_philosophers]
        go(dining_philosopher(phi_id, left, right, diner))
    await dining_manager(n_philosophers, diner)


# running the program

if __name__ == '__main__':
    run_in_thread(start_dining(PHILOSPHERS))
