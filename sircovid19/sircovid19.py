import numpy as np
import matplotlib.pyplot as plt
from scipy.integrate import odeint

def sir(beta, gamma):

    return


def base_seir_model(init_vals, params, t):
    S_0, E_0, I_0, R_0 = init_vals
    S, E, I, R = [S_0], [E_0], [I_0], [R_0]
    alpha, beta, gamma = params
    dt = t[1] - t[0]
    for d in t[1:]:
        next_S = S[-1] - (beta*S[-1]*I[-1])*dt
        next_E = E[-1] + (beta*S[-1]*I[-1] - alpha*E[-1])*dt
        next_I = I[-1] + (alpha*E[-1] - gamma*I[-1])*dt
        next_R = R[-1] + (gamma*I[-1])*dt
        S.append(next_S)
        E.append(next_E)
        I.append(next_I)
        R.append(next_R)
        print("Tag %d: S=%4.2f, E=%4.2f, I=%4.2f, R=%4.2f" % (d, next_S, next_E, next_I, next_R))

    return np.stack([S, E, I, R]).T


def seir_ode(y, t, beta, alpha, gamma):
    s, e, i, r = y
    d1 = -beta*i*s
    d2 = beta*i*s - alpha*e
    d3 = alpha*e - gamma*i
    d4 = gamma*i

    return d1, d2, d3, d4


def sird_ode(y, t, beta, gamma, mu):
    s, i, r, d = y
    d1 = -beta*i*s
    d2 = beta*i*s - gamma*i - mu*i
    d3 = gamma*i
    d4 = mu*i

    return d1, d2, d3, d4


if __name__ == '__main__':
    t_max = 100
    dt = 1
    t = np.linspace(0, t_max, int(t_max / dt) + 1)

    N = 10000
    # init_vals = 1 - 1/N, 1/N, 0, 0
    # init_vals = N-1, 1, 0, 0
    init_vals = 997, 3, 0, 0
    alpha = .2
    # beta = 1.75
    beta = .0004
    # gamma = 0.5
    gamma = .035
    params = alpha, beta, gamma
    # Run simulation
    results = base_seir_model(init_vals, params, t)

    fig, ax = plt.subplots()
    ax.plot(t, results[:, 0])
    ax.plot(t, results[:, 1])
    ax.plot(t, results[:, 2])
    ax.plot(t, results[:, 3])
    ax.legend(['S', 'E', 'I', 'R'])
    ax.set_title('Euler')

    results2 = odeint(seir_ode, init_vals, t, args=(beta, alpha, gamma))

    fig, ax = plt.subplots()
    ax.plot(t, results2[:, 0])
    ax.plot(t, results2[:, 1])
    ax.plot(t, results2[:, 2])
    ax.plot(t, results2[:, 3])
    ax.legend(['S', 'E', 'I', 'R'])
    ax.set_title('ODE')

    mu = .005

    results3 = odeint(sird_ode, init_vals, t, args=(beta, gamma, mu))

    fig, ax = plt.subplots()
    ax.plot(t, results3[:, 0])
    ax.plot(t, results3[:, 1])
    ax.plot(t, results3[:, 2])
    ax.plot(t, results3[:, 3])
    ax.legend(['S', 'I', 'R', 'D'])
    ax.set_title('ODE')
